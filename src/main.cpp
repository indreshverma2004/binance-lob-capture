#include "binance_capture.hpp"
#include "market_replay.hpp"
#include "metrics.hpp"
#include "rest_snapshot_client.hpp"
#include "sharding.hpp"

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
#include <ctime>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
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
    return "Usage: binance_capture --venue spot|usdm --symbols SYMBOL[,SYMBOL...] --output-dir PATH [--duration-seconds N]\n"
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
    size_t start = 0;
    while (true) {
        const size_t comma = arg.find(',', start);
        const std::string token = arg.substr(start, comma == std::string::npos ? comma : comma - start);
        if (token.empty()) {
            throw std::invalid_argument("--symbols contains an empty symbol");
        }
        const auto symbol = binance_capture::normalizeSymbol(token);
        if (!std::all_of(symbol.begin(), symbol.end(), [](unsigned char ch) { return std::isalnum(ch) != 0; })) {
            throw std::invalid_argument("--symbols may contain only letters and digits");
        }
        if (std::find(result.begin(), result.end(), symbol) != result.end()) {
            throw std::invalid_argument("--symbols contains duplicate symbol " + symbol);
        }
        result.push_back(symbol);
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return result;
}

std::string localDateStamp() {
    const std::time_t now = std::time(nullptr);
    std::tm local_time{};
#ifdef _WIN32
    if (localtime_s(&local_time, &now) != 0) throw std::runtime_error("unable to determine local date");
#else
    if (localtime_r(&now, &local_time) == nullptr) throw std::runtime_error("unable to determine local date");
#endif
    std::ostringstream output;
    output << std::put_time(&local_time, "%Y-%m-%d");
    return output.str();
}

std::string parseCaptureDuration(int argc, char** argv) {
    if (!hasArg(argc, argv, "--duration-seconds")) return {};
    const std::string value = readArg(argc, argv, "--duration-seconds");
    if (value.empty() || value.rfind("--", 0) == 0) {
        throw std::invalid_argument("--duration-seconds requires a positive integer");
    }
    size_t parsed = 0;
    const auto seconds = std::stoull(value, &parsed);
    if (parsed != value.size() || seconds == 0 || seconds > static_cast<unsigned long long>(std::chrono::seconds::max().count())) {
        throw std::invalid_argument("--duration-seconds requires a positive integer in range");
    }
    return std::to_string(seconds);
}

std::string streamPath(const std::string& venue, const std::vector<std::string>& symbols) {
    std::string query;
    for (const auto& symbol : symbols) {
        std::string lower = symbol;
        std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        if (!query.empty()) query.push_back('/');
        query += lower + "@depth@100ms/" + lower + "@depth5@100ms/" + lower + "@trade";
    }
    if (venue == "spot") return "/stream?streams=" + query;
    return "/public/stream?streams=" + query;
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

struct SymbolRuntime {
    explicit SymbolRuntime(const std::string& venue, const std::string& symbol)
        : processor(venue, symbol, binance_capture::OrderBookRecoveryMode::rest_snapshot) {}
    binance_capture::OrderBookProcessor processor;
    std::future<nlohmann::json> rest_future;
    bool rest_in_flight = false;
    bool discard_rest_result = false;
    uint32_t rest_attempt = 0;
    std::chrono::steady_clock::time_point next_rest_attempt = std::chrono::steady_clock::now();
    std::string resync_reason = "new WebSocket connection";
    bool depth5_warning = false;
};

void runShard(const std::string& venue,
              const std::vector<std::string>& symbols,
              int64_t shard_id,
              std::ofstream& market_csv,
              std::ofstream& orderbook_csv,
              std::mutex& output_mutex,
              uint64_t& global_orderbook_seq,
              binance_capture::MetricsCollector& metrics) {
    uint64_t conn_epoch = 0;
    uint64_t conn_seq = 0;
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

            const std::string host = venue == "spot" ? "stream.binance.com" : "fstream.binance.com";
            const std::string port = venue == "spot" ? "9443" : "443";
            const std::string path = streamPath(venue, symbols);
            std::cout << "Connecting shard=" << shard_id << " symbols=" << symbols.size()
                      << " to " << host << ':' << port << path << " (conn_epoch=" << conn_epoch << ")\n";
            tcp::resolver resolver(io);
            auto const results = resolver.resolve(host, port);
            ssl::stream<tcp::socket> stream(io, ctx);
            asio::connect(stream.lowest_layer(), results);
            stream.set_verify_mode(ssl::verify_peer);
            stream.set_verify_callback(ssl::host_name_verification(host));
            SSL_set_tlsext_host_name(stream.native_handle(), host.c_str());
            stream.handshake(ssl::stream_base::client);
            websocket::stream<ssl::stream<tcp::socket>> ws(std::move(stream));
            ws.handshake(host, path);
            std::cout << "WebSocket connected: shard=" << shard_id << '\n';

            std::map<std::string, std::unique_ptr<SymbolRuntime>> runtimes;
            for (const auto& symbol : symbols) {
                auto runtime = std::make_unique<SymbolRuntime>(venue, symbol);
                runtime->processor.beginResynchronization();
                runtimes.emplace(symbol, std::move(runtime));
            }

            auto startRest = [&](const std::string& symbol, SymbolRuntime& state) {
                if (state.rest_in_flight || g_stop_requested.load(std::memory_order_relaxed)) return;
                ++state.rest_attempt;
                std::cout << "REST resync request: shard=" << shard_id << " venue=" << venue
                          << " symbol=" << symbol << " attempt=" << state.rest_attempt
                          << " reason=" << state.resync_reason
                          << " buffered_events=" << state.processor.bufferedDepthEventCount() << '\n';
                try {
                    state.rest_future = std::async(std::launch::async, [venue, symbol]() {
                        return binance_capture::fetchRestDepthSnapshot(venue, symbol);
                    });
                    state.rest_in_flight = true;
                } catch (const std::exception& exc) {
                    metrics.recordRestSnapshotFailure();
                    const auto delay = binance_capture::restRetryDelaySeconds(state.rest_attempt);
                    std::cerr << "Unable to start REST snapshot request for " << symbol << ": "
                              << exc.what() << "; retrying in " << delay << " seconds.\n";
                    state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                }
            };

            auto writeRows = [&](const std::vector<binance_capture::OrderBookRow>& rows) {
                if (rows.empty()) return;
                std::lock_guard<std::mutex> lock(output_mutex);
                for (auto row : rows) {
                    row.seqNo = ++global_orderbook_seq;
                    binance_capture::writeOrderBookCsvRow(orderbook_csv, row);
                }
                orderbook_csv.flush();
            };

            auto pollRest = [&](const std::string& symbol, SymbolRuntime& state) {
                if (!state.rest_in_flight || state.rest_future.wait_for(std::chrono::seconds(0)) != std::future_status::ready) return;
                state.rest_in_flight = false;
                nlohmann::json snapshot;
                try {
                    snapshot = state.rest_future.get();
                } catch (const binance_capture::RestRequestError& exc) {
                    metrics.recordRestSnapshotFailure();
                    const auto delay = binance_capture::restRetryDelaySeconds(state.rest_attempt, exc.retryAfterSeconds());
                    std::cerr << "REST resync failed for " << symbol << ": " << exc.what()
                              << "; retrying in " << delay << " seconds.\n";
                    state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                    return;
                } catch (const std::exception& exc) {
                    metrics.recordRestSnapshotFailure();
                    const auto delay = binance_capture::restRetryDelaySeconds(state.rest_attempt);
                    std::cerr << "REST resync failed for " << symbol << ": " << exc.what()
                              << "; retrying in " << delay << " seconds.\n";
                    state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                    return;
                }
                if (state.discard_rest_result) {
                    state.discard_rest_result = false;
                    state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                    return;
                }
                try {
                    const auto recovery = state.processor.restoreFromRestSnapshot(snapshot);
                    if (!recovery.synchronized) {
                        state.resync_reason = recovery.reason;
                        const auto delay = binance_capture::restRetryDelaySeconds(state.rest_attempt);
                        state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                        std::cerr << "REST snapshot did not bridge buffered events for " << symbol
                                  << "; retrying in " << delay << " seconds.\n";
                        return;
                    }
                    writeRows(recovery.rows);
                    metrics.recordRecoveredDepthEvents(recovery.rows.size(), recovery.stale_events_discarded, recovery.rows.size());
                    metrics.recordRestResynchronization();
                    std::cout << "REST resync complete: shard=" << shard_id << " symbol=" << symbol
                              << " snapshot_seq=" << recovery.snapshot_update_id
                              << " applied_buffered=" << recovery.rows.size() << '\n';
                    state.rest_attempt = 0;
                    state.resync_reason.clear();
                } catch (const std::exception& exc) {
                    metrics.recordRestSnapshotFailure();
                    const auto delay = binance_capture::restRetryDelaySeconds(state.rest_attempt);
                    state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(delay);
                    std::cerr << "Invalid REST snapshot for " << symbol << ": " << exc.what()
                              << "; retrying in " << delay << " seconds.\n";
                }
            };

            for (auto& entry : runtimes) startRest(entry.first, *entry.second);
            while (!g_stop_requested.load(std::memory_order_relaxed)) {
                beast::flat_buffer buffer;
                ws.read(buffer);
                const auto now = std::chrono::system_clock::now();
                const auto t = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
                const auto tn = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch() - std::chrono::duration_cast<std::chrono::nanoseconds>(t));
                const int64_t recv_tsec = static_cast<int64_t>(t.count());
                const int32_t recv_tnsec = static_cast<int32_t>(tn.count());
                const auto doc = parseJsonOrThrow(beast::buffers_to_string(buffer.data()));
                if (!doc.is_object() || !doc.contains("stream") || !doc.contains("data") || !doc["data"].is_object()) {
                    throw std::runtime_error("Malformed combined stream message");
                }
                const std::string stream_name = doc["stream"].get<std::string>();
                const auto separator = stream_name.find('@');
                if (separator == std::string::npos) throw std::runtime_error("Malformed combined stream name");
                const std::string symbol = binance_capture::normalizeSymbol(stream_name.substr(0, separator));
                auto found = runtimes.find(symbol);
                if (found == runtimes.end()) throw std::runtime_error("Received stream for unassigned symbol " + symbol);
                auto& state = *found->second;
                const std::string kind = binance_capture::classifyStream(stream_name);
                const auto payload = doc["data"];
                const uint64_t event_seq = ++conn_seq;
                {
                    std::lock_guard<std::mutex> lock(output_mutex);
                    writeMarketDataRow(market_csv, venue, kind, shard_id, static_cast<int64_t>(conn_epoch), event_seq, symbol, payload, recv_tsec, recv_tnsec);
                    market_csv.flush();
                }
                const binance_capture::NormalizedMarketEvent event{recv_tsec, recv_tnsec, venue, kind,
                    shard_id, static_cast<int64_t>(conn_epoch), event_seq, symbol, payload};
                const auto processed = state.processor.process(event);
                metrics.observeEvent(event, processed);
                if (processed.disposition == binance_capture::EventDisposition::gap) {
                    state.resync_reason = "differential sequence gap";
                    std::cerr << "Depth sequence gap: shard=" << shard_id << " symbol=" << symbol << '\n';
                } else if (processed.disposition == binance_capture::EventDisposition::depth5_mismatch && !state.depth5_warning) {
                    state.depth5_warning = true;
                    std::cerr << "Depth5 view differs from REST-backed book: shard=" << shard_id << " symbol=" << symbol << '\n';
                } else if (processed.disposition == binance_capture::EventDisposition::buffer_overflow) {
                    state.discard_rest_result = state.rest_in_flight;
                    state.rest_attempt = 0;
                    state.resync_reason = "depth event buffer overflow";
                    state.next_rest_attempt = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                }
                if (processed.order_book_row) writeRows({*processed.order_book_row});
                for (auto& entry : runtimes) {
                    auto& candidate = *entry.second;
                    pollRest(entry.first, candidate);
                    if (candidate.processor.isResynchronizing() && !candidate.rest_in_flight &&
                        std::chrono::steady_clock::now() >= candidate.next_rest_attempt) {
                        startRest(entry.first, candidate);
                    }
                }
            }
            if (g_stop_requested.load(std::memory_order_relaxed)) {
                beast::error_code close_error;
                ws.close(websocket::close_code::normal, close_error);
            }
        } catch (const std::exception& exc) {
            std::cerr << "Connection error on shard=" << shard_id << ": " << exc.what() << '\n';
            if (g_stop_requested.load(std::memory_order_relaxed)) break;
            metrics.recordReconnect();
            ++conn_epoch;
            conn_seq = 0;
            std::cout << "Reconnecting shard=" << shard_id << " in 2 seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
}

int main(int argc, char** argv) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
        if (hasArg(argc, argv, "--replay")) {
            if (hasArg(argc, argv, "--venue") || hasArg(argc, argv, "--symbols") || hasArg(argc, argv, "--duration-seconds")) {
                throw std::invalid_argument("--replay cannot be combined with --venue, --symbols, or --duration-seconds");
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
        const std::string duration_arg = parseCaptureDuration(argc, argv);
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
        const auto shards = binance_capture::shardSymbols(symbols);

        std::filesystem::create_directories(output_dir_arg);
        std::string symbol_label;
        for (const auto& symbol : symbols) {
            if (!symbol_label.empty()) symbol_label.push_back('-');
            symbol_label += symbol;
        }
        const std::string file_stem = "market_data_" + venue + "_" + symbol_label + "_" + localDateStamp();
        const auto market_file = std::filesystem::path(output_dir_arg) / (file_stem + ".csv");
        const auto orderbook_file = std::filesystem::path(output_dir_arg) / (file_stem + "_orderbook.csv");
        if (std::filesystem::exists(market_file) || std::filesystem::exists(orderbook_file)) {
            throw std::runtime_error("capture output already exists; choose another output directory or remove/rename the existing pair: " + file_stem);
        }
        const std::string market_path = market_file.string();
        const std::string orderbook_path = orderbook_file.string();
        std::ofstream market_csv(market_path, std::ios::out | std::ios::binary);
        if (!market_csv) throw std::runtime_error("Unable to create market data output: " + market_path);
        market_csv << binance_capture::buildMarketDataHeader() << '\n';
        std::ofstream orderbook_csv(orderbook_path, std::ios::out | std::ios::binary);
        if (!orderbook_csv) throw std::runtime_error("Unable to create order-book output: " + orderbook_path);
        binance_capture::writeOrderBookCsvHeader(orderbook_csv);

        std::cout << "Starting Binance capture: venue=" << venue << " symbols=" << symbols.size()
                  << " shards=" << shards.size() << " output_dir=" << output_dir_arg << '\n';
        for (size_t i = 0; i < shards.size(); ++i) {
            std::cout << "shard " << i << ':';
            for (const auto& symbol : shards[i]) std::cout << ' ' << symbol;
            std::cout << '\n';
        }
        std::vector<binance_capture::MetricsCollector> shard_metrics(shards.size());
        std::vector<std::thread> workers;
        std::mutex output_mutex;
        uint64_t global_orderbook_seq = 0;
        const auto capture_started = std::chrono::steady_clock::now();
        std::thread duration_timer;
        if (!duration_arg.empty()) {
            const auto duration = std::chrono::seconds(std::stoull(duration_arg));
            duration_timer = std::thread([duration]() {
                const auto deadline = std::chrono::steady_clock::now() + duration;
                while (!g_stop_requested.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (std::chrono::steady_clock::now() >= deadline) {
                    g_stop_requested.store(true, std::memory_order_relaxed);
                }
            });
        }
        workers.reserve(shards.size());
        for (size_t i = 0; i < shards.size(); ++i) {
            workers.emplace_back(runShard, venue, std::cref(shards[i]), static_cast<int64_t>(i),
                                 std::ref(market_csv), std::ref(orderbook_csv), std::ref(output_mutex),
                                 std::ref(global_orderbook_seq), std::ref(shard_metrics[i]));
        }
        for (auto& worker : workers) worker.join();
        if (duration_timer.joinable()) duration_timer.join();
        market_csv.close();
        orderbook_csv.close();
        const double elapsed_seconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - capture_started).count();
        binance_capture::MetricsSnapshot counts;
        for (const auto& collector : shard_metrics) {
            const auto part = collector.snapshot();
            counts.total_events += part.total_events;
            counts.trade_events += part.trade_events;
            counts.depth_diff_events += part.depth_diff_events;
            counts.depth5_events += part.depth5_events;
            counts.order_book_rows += part.order_book_rows;
            counts.depth_diffs_applied += part.depth_diffs_applied;
            counts.stale_depth_diffs += part.stale_depth_diffs;
            counts.zero_quantity_updates += part.zero_quantity_updates;
            counts.reconnects += part.reconnects;
            counts.rest_resynchronizations += part.rest_resynchronizations;
            counts.rest_snapshot_failures += part.rest_snapshot_failures;
        }
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
