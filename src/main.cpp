#include "binance_capture.hpp"
#include "market_replay.hpp"
#include "metrics.hpp"
#include "rest_snapshot_client.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <chrono>
#include <cctype>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

static std::atomic<bool> g_stop_requested{false};

void signalHandler(int) {
    g_stop_requested.store(true, std::memory_order_relaxed);
}

std::string usage() {
    return "Usage: binance_capture --venue spot|usdm --symbols SYMBOL --output-dir PATH\n"
           "   or: binance_capture --replay MARKET_CSV --output-dir PATH [--benchmark]";
}

bool hasArg(int argc, char** argv, const std::string& key) {
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == key) {
            return true;
        }
    }
    return false;
}

std::string readArg(int argc, char** argv, const std::string& key) {
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == key && i + 1 < argc) {
            return argv[i + 1];
        }
    }
    return "";
}

std::string parseVenue(const std::string& venue) {
    if (venue == "spot" || venue == "usdm") {
        return venue;
    }
    throw std::invalid_argument("--venue must be spot or usdm");
}

std::vector<std::string> splitSymbols(const std::string& arg) {
    std::vector<std::string> result;
    std::stringstream ss(arg);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            result.push_back(binance_capture::normalizeSymbol(token));
        }
    }
    return result;
}

nlohmann::json parseJsonOrThrow(const std::string& raw) {
    try {
        return nlohmann::json::parse(raw);
    } catch (const std::exception& exc) {
        throw std::runtime_error(std::string("invalid JSON: ") + exc.what());
    }
}

void writeMarketDataRow(std::ofstream& market_csv,
                        const std::string& venue,
                        const std::string& stream_kind,
                        int64_t shard_id,
                        int64_t conn_epoch,
                        uint64_t conn_seq,
                        const std::string& symbol,
                        const nlohmann::json& payload,
                        int64_t recv_tsec,
                        int32_t recv_tnsec) {
    const std::string payload_json = binance_capture::compactJson(payload);
    market_csv << recv_tsec << ','
               << recv_tnsec << ','
               << venue << ','
               << stream_kind << ','
               << shard_id << ','
               << conn_epoch << ','
               << conn_seq << ','
               << binance_capture::normalizeSymbol(symbol) << ','
               << binance_capture::csvEscape(payload_json)
               << '\n';
}

int main(int argc, char** argv) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
        if (hasArg(argc, argv, "--replay")) {
            if (hasArg(argc, argv, "--venue") || hasArg(argc, argv, "--symbols")) {
                throw std::invalid_argument("--replay cannot be combined with --venue or --symbols");
            }
            const std::string replay_path = readArg(argc, argv, "--replay");
            const std::string output_dir_arg = readArg(argc, argv, "--output-dir");
            if (replay_path.empty() || replay_path.rfind("--", 0) == 0) {
                throw std::invalid_argument("--replay requires a market_data.csv path");
            }
            if (output_dir_arg.empty()) {
                throw std::invalid_argument("--output-dir is required with --replay");
            }

            std::ifstream market_csv(replay_path, std::ios::in | std::ios::binary);
            if (!market_csv) {
                throw std::runtime_error("unable to open replay input: " + replay_path);
            }
            const auto input_path = std::filesystem::weakly_canonical(replay_path);
            std::filesystem::create_directories(output_dir_arg);
            const auto output_dir = std::filesystem::weakly_canonical(output_dir_arg);
            if (input_path.parent_path() == output_dir) {
                throw std::invalid_argument("replay output must use a directory separate from the input capture");
            }
            const auto orderbook_path = output_dir / "order_book.csv";
            if (std::filesystem::exists(orderbook_path)) {
                throw std::runtime_error("replay output already exists; choose an unused output directory: " + orderbook_path.string());
            }
            std::ofstream orderbook_csv(orderbook_path, std::ios::out | std::ios::binary);
            if (!orderbook_csv) {
                throw std::runtime_error("unable to create replay order_book.csv: " + orderbook_path.string());
            }

            const bool benchmark = hasArg(argc, argv, "--benchmark");
            binance_capture::MetricsCollector replay_metrics;
            const auto benchmark_started = std::chrono::steady_clock::now();
            const auto stats = binance_capture::replayMarketDataCsv(
                market_csv, orderbook_csv, benchmark ? &replay_metrics : nullptr);
            orderbook_csv.flush();
            if (!orderbook_csv) {
                throw std::runtime_error("failed flushing replay order_book.csv");
            }
            std::cout << "Replay complete: market_events=" << stats.market_events
                      << " depth_diff=" << stats.depth_diff_events
                      << " depth5=" << stats.depth5_events
                      << " trade=" << stats.trade_events
                      << " applied_diff=" << stats.applied_diff_events
                      << " stale_diff=" << stats.stale_diff_events
                      << " gaps=" << stats.gap_events
                      << " ignored_out_of_sync=" << stats.ignored_out_of_sync_events
                      << " order_book_rows=" << stats.order_book_rows
                      << " rejected=0 output=" << orderbook_path.string() << '\n';
            if (benchmark) {
                const double elapsed_seconds = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - benchmark_started).count();
                const auto counts = replay_metrics.snapshot();
                std::cout << "=== Replay Benchmark ===\n"
                          << std::fixed << std::setprecision(2)
                          << "Input events:       " << stats.market_events << '\n'
                          << "Order-book rows:    " << stats.order_book_rows << '\n'
                          << "Elapsed:            " << elapsed_seconds << " s\n"
                          << "Events/sec:         " << binance_capture::MetricsCollector::ratePerSecond(
                                 counts.total_events, elapsed_seconds) << '\n'
                          << "Rows/sec:           " << binance_capture::MetricsCollector::ratePerSecond(
                                 counts.order_book_rows, elapsed_seconds) << '\n';
            }
            return 0;
        }

        if (hasArg(argc, argv, "--benchmark")) {
            throw std::invalid_argument("--benchmark requires --replay");
        }

        const std::string venue_arg = readArg(argc, argv, "--venue");
        const std::string symbols_arg = readArg(argc, argv, "--symbols");
        const std::string output_dir_arg = readArg(argc, argv, "--output-dir");
        if (venue_arg.empty() || symbols_arg.empty() || output_dir_arg.empty()) {
            std::cerr << usage() << '\n';
            return 2;
        }

        const std::string venue = parseVenue(venue_arg);
        const std::vector<std::string> symbols = splitSymbols(symbols_arg);
        if (symbols.empty()) {
            std::cerr << "No valid symbols were provided.\n";
            return 2;
        }
        if (symbols.size() != 1) {
            std::cerr << "This build supports one symbol per run.\n";
            return 2;
        }

        const std::string symbol = symbols.front();

        std::filesystem::create_directories(output_dir_arg);
        const std::string market_path = (std::filesystem::path(output_dir_arg) / "market_data.csv").string();
        const std::string orderbook_path = (std::filesystem::path(output_dir_arg) / "order_book.csv").string();

        std::ofstream market_csv(market_path, std::ios::out | std::ios::binary);
        if (!market_csv) {
            std::cerr << "Unable to open market_data.csv for writing: " << market_path << '\n';
            return 1;
        }
        market_csv << binance_capture::buildMarketDataHeader() << '\n';

        std::ofstream orderbook_csv(orderbook_path, std::ios::out | std::ios::binary);
        if (!orderbook_csv) {
            std::cerr << "Unable to open order_book.csv for writing: " << orderbook_path << '\n';
            return 1;
        }
        binance_capture::writeOrderBookCsvHeader(orderbook_csv);

        std::cout << "Starting Binance capture for venue=" << venue << " symbol=" << symbol << " output_dir=" << output_dir_arg << '\n';

        uint64_t conn_epoch = 0;
        uint64_t conn_seq = 0;
        binance_capture::OrderBookProcessor orderbook_processor(
            venue, symbol, binance_capture::OrderBookRecoveryMode::rest_snapshot);
        binance_capture::MetricsCollector capture_metrics;
        const auto capture_started = std::chrono::steady_clock::now();

        while (!g_stop_requested.load(std::memory_order_relaxed)) {
            try {
                asio::io_context io;
                ssl::context ctx(ssl::context::sslv23_client);
                ctx.set_verify_mode(ssl::verify_peer);
                ctx.set_default_verify_paths();
                if (std::filesystem::exists("C:/msys64/usr/ssl/certs/ca-bundle.crt")) {
                    ctx.load_verify_file("C:/msys64/usr/ssl/certs/ca-bundle.crt");
                } else if (std::filesystem::exists("C:/msys64/usr/ssl/cert.pem")) {
                    ctx.load_verify_file("C:/msys64/usr/ssl/cert.pem");
                }

                tcp::resolver resolver(io);
                std::string host;
                std::string port;
                std::string path;
                std::string stream_symbol = symbol;
                std::transform(stream_symbol.begin(), stream_symbol.end(), stream_symbol.begin(), [](unsigned char ch) {
                    return static_cast<char>(std::tolower(ch));
                });
                if (venue == "spot") {
                    host = "stream.binance.com";
                    port = "9443";
                    path = "/stream?streams=" + stream_symbol + "@depth@100ms/" + stream_symbol + "@depth5@100ms/" + stream_symbol + "@trade";
                } else {
                    host = "fstream.binance.com";
                    port = "443";
                    path = "/public/stream?streams=" + stream_symbol + "@depth@100ms/" + stream_symbol + "@depth5@100ms/" + stream_symbol + "@trade";
                }

                std::cout << "Connecting to " << host << ':' << port << path << " (conn_epoch=" << conn_epoch << ")\n";
                auto const results = resolver.resolve(host, port);

                ssl::stream<tcp::socket> stream(io, ctx);
                asio::connect(stream.lowest_layer(), results);
                stream.set_verify_mode(ssl::verify_peer);
                stream.set_verify_callback(ssl::host_name_verification(host));
                SSL_set_tlsext_host_name(stream.native_handle(), host.c_str());
                stream.handshake(ssl::stream_base::client);

                websocket::stream<ssl::stream<tcp::socket>> ws(std::move(stream));
                ws.handshake(host, path);

                std::cout << "WebSocket connected.\n";
                orderbook_processor.beginResynchronization();
                std::future<nlohmann::json> rest_future;
                bool rest_request_in_flight = false;
                bool discard_rest_result = false;
                uint32_t rest_attempt = 0;
                auto next_rest_attempt = std::chrono::steady_clock::now();
                std::string resync_reason = "new WebSocket connection";
                bool depth5_sanity_warning_logged = false;

                auto startRestRequest = [&]() {
                    if (rest_request_in_flight || g_stop_requested.load(std::memory_order_relaxed)) {
                        return;
                    }
                    ++rest_attempt;
                    std::cout << "REST resync request: venue=" << venue << " symbol=" << symbol
                              << " attempt=" << rest_attempt << " reason=" << resync_reason
                              << " buffered_events=" << orderbook_processor.bufferedDepthEventCount() << '\n';
                    try {
                        rest_future = std::async(std::launch::async, [venue, symbol]() {
                            return binance_capture::fetchRestDepthSnapshot(venue, symbol);
                        });
                        rest_request_in_flight = true;
                    } catch (const std::exception& exc) {
                        capture_metrics.recordRestSnapshotFailure();
                        const auto delay = binance_capture::restRetryDelaySeconds(rest_attempt);
                        std::cerr << "Unable to start REST snapshot request: " << exc.what()
                                  << "; retrying in " << delay << " seconds.\n";
                        next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                    }
                };

                auto writeRecoveredRows = [&](const std::vector<binance_capture::OrderBookRow>& rows) {
                    for (const auto& row : rows) {
                        binance_capture::writeOrderBookCsvRow(orderbook_csv, row);
                        orderbook_csv.flush();
                    }
                };

                auto pollRestRequest = [&]() {
                    if (!rest_request_in_flight ||
                        rest_future.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
                        return;
                    }
                    rest_request_in_flight = false;
                    nlohmann::json snapshot_payload;
                    try {
                        snapshot_payload = rest_future.get();
                    } catch (const binance_capture::RestRequestError& exc) {
                        capture_metrics.recordRestSnapshotFailure();
                        if (discard_rest_result) {
                            discard_rest_result = false;
                            next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                            std::cerr << "Discarded REST request after resync buffer overflow: " << exc.what() << '\n';
                            return;
                        }
                        const auto delay = binance_capture::restRetryDelaySeconds(rest_attempt, exc.retryAfterSeconds());
                        std::cerr << "REST resync failed: " << exc.what() << "; venue=" << venue
                                  << " symbol=" << symbol << "; retrying in " << delay << " seconds.\n";
                        next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                        return;
                    } catch (const std::exception& exc) {
                        capture_metrics.recordRestSnapshotFailure();
                        if (discard_rest_result) {
                            discard_rest_result = false;
                            next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                            std::cerr << "Discarded REST request after resync buffer overflow: " << exc.what() << '\n';
                            return;
                        }
                        const auto delay = binance_capture::restRetryDelaySeconds(rest_attempt);
                        std::cerr << "REST resync failed: " << exc.what() << "; venue=" << venue
                                  << " symbol=" << symbol << "; retrying in " << delay << " seconds.\n";
                        next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                        return;
                    }

                    if (discard_rest_result) {
                        discard_rest_result = false;
                        next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                        std::cout << "Discarded REST snapshot after resync buffer overflow; buffered_events="
                                  << orderbook_processor.bufferedDepthEventCount() << '\n';
                        return;
                    }

                    try {
                        const auto recovery = orderbook_processor.restoreFromRestSnapshot(snapshot_payload);
                        if (!recovery.synchronized) {
                            const auto delay = binance_capture::restRetryDelaySeconds(rest_attempt);
                            resync_reason = recovery.reason;
                            std::cerr << "REST snapshot sequence did not bridge buffered events: venue=" << venue
                                      << " symbol=" << symbol << " snapshot_seq=" << recovery.snapshot_update_id
                                      << " buffered_events=" << recovery.buffered_events << "; retrying in "
                                      << delay << " seconds.\n";
                            next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                            return;
                        }
                        writeRecoveredRows(recovery.rows);
                        capture_metrics.recordRecoveredDepthEvents(
                            recovery.rows.size(), recovery.stale_events_discarded, recovery.rows.size());
                        capture_metrics.recordRestResynchronization();
                        std::cout << "REST resync complete: venue=" << venue << " symbol=" << symbol
                                  << " snapshot_seq=" << recovery.snapshot_update_id
                                  << " buffered_events=" << recovery.buffered_events
                                  << " stale_discarded=" << recovery.stale_events_discarded
                                  << " applied_buffered=" << recovery.rows.size() << '\n';
                        rest_attempt = 0;
                        resync_reason.clear();
                    } catch (const std::exception& exc) {
                        capture_metrics.recordRestSnapshotFailure();
                        const auto delay = binance_capture::restRetryDelaySeconds(rest_attempt);
                        std::cerr << "Invalid REST depth snapshot: " << exc.what() << "; venue=" << venue
                                  << " symbol=" << symbol << "; retrying in " << delay << " seconds.\n";
                        next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                    }
                };

                startRestRequest();
                while (!g_stop_requested.load(std::memory_order_relaxed)) {
                    beast::flat_buffer buffer;
                    ws.read(buffer);
                    const auto now = std::chrono::system_clock::now();
                    const auto t = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
                    const auto tn = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch() - std::chrono::duration_cast<std::chrono::nanoseconds>(t));
                    const int64_t recv_tsec = static_cast<int64_t>(t.count());
                    const int32_t recv_tnsec = static_cast<int32_t>(tn.count());

                    const std::string raw = beast::buffers_to_string(buffer.data());
                    const auto doc = parseJsonOrThrow(raw);
                    if (!doc.is_object() || !doc.contains("stream") || !doc.contains("data")) {
                        throw std::runtime_error("Malformed combined stream message");
                    }
                    if (!doc["data"].is_object()) {
                        throw std::runtime_error("Combined stream data must be an object");
                    }

                    const std::string stream_name = doc["stream"].get<std::string>();
                    const std::string stream_kind = binance_capture::classifyStream(stream_name);
                    const auto payload = doc["data"];

                    ++conn_seq;
                    writeMarketDataRow(market_csv, venue, stream_kind, 0, conn_epoch, conn_seq, symbol, payload, recv_tsec, recv_tnsec);
                    market_csv.flush();

                    const binance_capture::NormalizedMarketEvent event{
                        recv_tsec,
                        recv_tnsec,
                        venue,
                        stream_kind,
                        0,
                        static_cast<int64_t>(conn_epoch),
                        conn_seq,
                        symbol,
                        payload
                    };
                    const auto processed = orderbook_processor.process(event);
                    capture_metrics.observeEvent(event, processed);
                    if (processed.disposition == binance_capture::EventDisposition::gap) {
                        resync_reason = "differential sequence gap";
                        std::cerr << "Depth sequence gap; buffering updates and requesting REST snapshot.\n";
                    } else if (processed.disposition == binance_capture::EventDisposition::depth5_mismatch) {
                        if (!depth5_sanity_warning_logged) {
                            std::cerr << "Depth5 partial view differs from REST-backed book; retained REST/diff state.\n";
                            depth5_sanity_warning_logged = true;
                        }
                    } else if (processed.disposition == binance_capture::EventDisposition::buffer_overflow) {
                        discard_rest_result = rest_request_in_flight;
                        rest_attempt = 0;
                        resync_reason = "depth event buffer overflow";
                        next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                        std::cerr << "REST resync buffer overflow; discarded buffer and will fetch a fresh snapshot.\n";
                    }
                    if (processed.order_book_row) {
                        binance_capture::writeOrderBookCsvRow(orderbook_csv, *processed.order_book_row);
                        orderbook_csv.flush();
                    }

                    pollRestRequest();
                    if (orderbook_processor.isResynchronizing() && !rest_request_in_flight &&
                        std::chrono::steady_clock::now() >= next_rest_attempt) {
                        startRestRequest();
                    }
                }
                if (g_stop_requested.load(std::memory_order_relaxed)) {
                    beast::error_code close_error;
                    ws.close(websocket::close_code::normal, close_error);
                }
            } catch (const std::exception& exc) {
                std::cerr << "Connection error: " << exc.what() << '\n';
                if (g_stop_requested.load(std::memory_order_relaxed)) {
                    break;
                }
                capture_metrics.recordReconnect();
                ++conn_epoch;
                conn_seq = 0;
                orderbook_processor.beginResynchronization();
                std::cout << "Reconnecting in 2 seconds...\n";
                std::this_thread::sleep_for(std::chrono::seconds(2));
                if (g_stop_requested.load(std::memory_order_relaxed)) {
                    break;
                }
            }
        }

        market_csv.close();
        orderbook_csv.close();
        const double elapsed_seconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - capture_started).count();
        const auto counts = capture_metrics.snapshot();
        std::cout << "=== Capture Metrics ===\n"
                  << std::fixed << std::setprecision(2)
                  << "Runtime:              " << elapsed_seconds << " s\n"
                  << "Total events:         " << counts.total_events << '\n'
                  << "Trade events:         " << counts.trade_events << '\n'
                  << "Depth diff events:    " << counts.depth_diff_events << '\n'
                  << "Depth5 events:        " << counts.depth5_events << '\n'
                  << "Order-book rows:      " << counts.order_book_rows << '\n'
                  << "Diffs applied:        " << counts.depth_diffs_applied << '\n'
                  << "Stale diffs:          " << counts.stale_depth_diffs << '\n'
                  << "Zero-quantity updates: " << counts.zero_quantity_updates << '\n'
                  << "Reconnects:           " << counts.reconnects << '\n'
                  << "REST resyncs:         " << counts.rest_resynchronizations << '\n'
                  << "REST failures:        " << counts.rest_snapshot_failures << '\n'
                  << "Events/sec:           " << binance_capture::MetricsCollector::ratePerSecond(
                         counts.total_events, elapsed_seconds) << '\n';
        std::cout << "Shutdown complete.\n";
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << "Fatal error: " << exc.what() << '\n';
        return 1;
    }
}
