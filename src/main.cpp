#include "binance_capture.hpp"

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
#include <iostream>
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
    return "Usage: binance_capture --venue spot|usdm --symbols SYMBOL --output-dir PATH";
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

std::vector<binance_capture::DepthLevel> parseDepthArray(const nlohmann::json& value, const std::string& field_name) {
    std::vector<binance_capture::DepthLevel> result;
    if (!value.contains(field_name)) {
        return result;
    }
    const auto& array = value[field_name];
    if (!array.is_array()) {
        throw std::runtime_error("Malformed Binance depth array for " + field_name);
    }
    for (const auto& item : array) {
        if (!item.is_array() || item.size() < 2) {
            throw std::runtime_error("Depth array item missing required price/quantity fields");
        }
        std::string price_text = item[0].get<std::string>();
        std::string qty_text = item[1].get<std::string>();
        result.push_back({
            binance_capture::scaledIntegerFromString(price_text, binance_capture::PRICE_SCALE, field_name + ".price"),
            binance_capture::scaledIntegerFromString(qty_text, binance_capture::QTY_SCALE, field_name + ".qty")
        });
    }
    return result;
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

void writeOrderBookRow(std::ofstream& orderbook_csv,
                       const binance_capture::OrderBook& book,
                       int64_t tsec,
                       int32_t tnsec,
                       uint64_t seqNo,
                       int32_t id,
                       char type,
                       char side) {
    const auto top = book.topFive();
    orderbook_csv << tsec << ',' << tnsec << ',' << seqNo << ',' << id << ',' << type << ',' << side;
    for (int i = 0; i < 5; ++i) {
        orderbook_csv << ',' << top.bid_prices[i];
    }
    for (int i = 0; i < 5; ++i) {
        orderbook_csv << ',' << top.bid_sizes[i];
    }
    for (int i = 0; i < 5; ++i) {
        orderbook_csv << ',' << top.ask_prices[i];
    }
    for (int i = 0; i < 5; ++i) {
        orderbook_csv << ',' << top.ask_sizes[i];
    }
    orderbook_csv << '\n';
}

int main(int argc, char** argv) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
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
        orderbook_csv << binance_capture::buildOrderBookHeader() << '\n';

        std::cout << "Starting Binance capture for venue=" << venue << " symbol=" << symbol << " output_dir=" << output_dir_arg << '\n';

        uint64_t conn_epoch = 0;
        uint64_t conn_seq = 0;
        uint64_t orderbook_seq = 0;
        uint64_t last_depth_seq = 0;
        bool have_depth_baseline = false;
        bool book_out_of_sync = true;
        binance_capture::OrderBook order_book;

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

                    if (stream_kind == "depth_diff") {
                        const auto bid_levels = parseDepthArray(payload, "b");
                        const auto ask_levels = parseDepthArray(payload, "a");
                        if (book_out_of_sync || !have_depth_baseline) {
                            continue;
                        }
                        const auto sequence_status = binance_capture::applyDepthSequence(venue, payload, last_depth_seq);
                        if (sequence_status == binance_capture::DepthSequenceStatus::gap) {
                            book_out_of_sync = true;
                            have_depth_baseline = false;
                            std::cerr << "Depth sequence gap; waiting for depth5 refresh.\n";
                            continue;
                        }
                        if (sequence_status == binance_capture::DepthSequenceStatus::stale) {
                            continue;
                        }
                        order_book.applyDepthDiff(bid_levels, ask_levels);
                        ++orderbook_seq;
                        writeOrderBookRow(orderbook_csv, order_book, recv_tsec, recv_tnsec, orderbook_seq, static_cast<int32_t>(binance_capture::stableInstrumentId(symbol)), 'D', 'N');
                        orderbook_csv.flush();
                    } else if (stream_kind == "depth5") {
                        const auto bid_levels = parseDepthArray(payload, "bids");
                        const auto ask_levels = parseDepthArray(payload, "asks");
                        order_book.applyDepth5(bid_levels, ask_levels);
                        last_depth_seq = binance_capture::depthSnapshotUpdateId(payload);
                        have_depth_baseline = true;
                        book_out_of_sync = false;
                        ++orderbook_seq;
                        writeOrderBookRow(orderbook_csv, order_book, recv_tsec, recv_tnsec, orderbook_seq, static_cast<int32_t>(binance_capture::stableInstrumentId(symbol)), 'S', 'N');
                        orderbook_csv.flush();
                    } else if (stream_kind == "trade") {
                        // Trade events are written to market_data.csv only and do not modify the local book.
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
                ++conn_epoch;
                conn_seq = 0;
                last_depth_seq = 0;
                have_depth_baseline = false;
                book_out_of_sync = true;
                order_book.bids.clear();
                order_book.asks.clear();
                std::cout << "Reconnecting in 2 seconds...\n";
                std::this_thread::sleep_for(std::chrono::seconds(2));
                if (g_stop_requested.load(std::memory_order_relaxed)) {
                    break;
                }
            }
        }

        market_csv.close();
        orderbook_csv.close();
        std::cout << "Shutdown complete.\n";
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << "Fatal error: " << exc.what() << '\n';
        return 1;
    }
}
