#include "binance_capture.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace binance_capture {

namespace {

std::vector<std::string> splitStreamName(const std::string& stream_name) {
    std::vector<std::string> parts;
    std::string part;
    std::stringstream ss(stream_name);
    while (std::getline(ss, part, '/')) {
        if (!part.empty()) {
            parts.push_back(part);
        }
    }
    return parts;
}

std::string formatInt64(int64_t value) {
    return std::to_string(value);
}

}  // namespace

std::string csvEscape(const std::string& value) {
    if (value.find_first_of(",\"\r\n") == std::string::npos) {
        return value;
    }
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped.push_back('"');
    for (char ch : value) {
        if (ch == '"') {
            escaped += "\"\"";
        } else {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('"');
    return escaped;
}

std::string normalizeSymbol(const std::string& symbol) {
    std::string result = symbol;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return result;
}

std::string buildStreamQuery(const std::string& venue, const std::string& symbol) {
    const std::string normalized_symbol = normalizeSymbol(symbol);
    std::string query = normalized_symbol + "@depth@100ms/" + normalized_symbol + "@depth5@100ms/" + normalized_symbol + "@trade";
    if (venue == "spot") {
        return "wss://stream.binance.com:9443/stream?streams=" + query;
    }
    if (venue == "usdm") {
        return "wss://fstream.binance.com/public/stream?streams=" + query;
    }
    throw std::invalid_argument("venue must be spot or usdm");
}

std::string buildMarketDataHeader() {
    return "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json";
}

std::string buildOrderBookHeader() {
    return "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4";
}

std::string compactJson(const nlohmann::json& value) {
    return value.dump();
}

int64_t scaledIntegerFromString(const std::string& text, int64_t scale, const std::string& label) {
    if (text.empty()) {
        throw std::invalid_argument(label + " is empty");
    }
    if (scale <= 0) {
        throw std::invalid_argument(label + " scale must be a positive power of ten");
    }

    size_t precision = 0;
    int64_t scale_check = scale;
    while (scale_check > 1 && scale_check % 10 == 0) {
        scale_check /= 10;
        ++precision;
    }
    if (scale_check != 1) {
        throw std::invalid_argument(label + " scale must be a positive power of ten");
    }

    std::string sanitized = text;
    bool negative = false;
    if (!sanitized.empty() && sanitized.front() == '-') {
        negative = true;
        sanitized.erase(sanitized.begin());
    }

    if (sanitized.empty()) {
        throw std::invalid_argument(label + " is invalid");
    }

    std::string int_part;
    std::string frac_part;
    const auto dot_pos = sanitized.find('.');
    if (dot_pos == std::string::npos) {
        int_part = sanitized;
    } else {
        int_part = sanitized.substr(0, dot_pos);
        frac_part = sanitized.substr(dot_pos + 1);
    }

    if (int_part.empty()) {
        int_part = "0";
    }

    for (char ch : int_part) {
        if (ch < '0' || ch > '9') {
            throw std::invalid_argument(label + " contains an invalid integer part");
        }
    }
    for (char ch : frac_part) {
        if (ch < '0' || ch > '9') {
            throw std::invalid_argument(label + " contains an invalid fractional part");
        }
    }

    if (frac_part.size() > precision) {
        const auto extra = frac_part.substr(precision);
        if (extra.find_first_not_of('0') != std::string::npos) {
            throw std::invalid_argument(label + " has more decimal places than its scale supports");
        }
        frac_part.resize(precision);
    }

    std::string scaled_string = int_part;
    scaled_string += frac_part + std::string(precision - frac_part.size(), '0');

    const auto first_nonzero = scaled_string.find_first_not_of('0');
    if (first_nonzero == std::string::npos) {
        return 0;
    }
    scaled_string.erase(0, first_nonzero);

    const uint64_t limit = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + (negative ? 1u : 0u);
    uint64_t result = 0;
    for (char ch : scaled_string) {
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        if (result > (limit - digit) / 10) {
            throw std::overflow_error(label + " overflow");
        }
        result = result * 10 + digit;
    }

    if (negative && result == static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1u) {
        return std::numeric_limits<int64_t>::min();
    }
    const auto signed_result = static_cast<int64_t>(result);
    return negative ? -signed_result : signed_result;
}

uint32_t stableInstrumentId(const std::string& symbol) {
    const std::string normalized = normalizeSymbol(symbol);
    uint32_t hash = 2166136261u;
    for (unsigned char ch : normalized) {
        hash ^= static_cast<uint32_t>(ch);
        hash *= 16777619u;
    }
    return hash % 2147483647u;
}

std::string classifyStream(const std::string& stream_name) {
    if (stream_name.find("@depth@100ms") != std::string::npos) {
        return "depth_diff";
    }
    if (stream_name.find("@depth5@100ms") != std::string::npos) {
        return "depth5";
    }
    if (stream_name.find("@trade") != std::string::npos) {
        return "trade";
    }
    throw std::invalid_argument("Unsupported stream kind: " + stream_name);
}

void OrderBook::applyDepthDiff(const std::vector<DepthLevel>& bid_updates,
                               const std::vector<DepthLevel>& ask_updates) {
    for (const auto& update : bid_updates) {
        if (update.qty > 0) {
            bids[update.price] = update.qty;
        } else {
            bids.erase(update.price);
        }
    }
    for (const auto& update : ask_updates) {
        if (update.qty > 0) {
            asks[update.price] = update.qty;
        } else {
            asks.erase(update.price);
        }
    }
}

void OrderBook::applyDepth5(const std::vector<DepthLevel>& bid_updates,
                            const std::vector<DepthLevel>& ask_updates) {
    bids.clear();
    asks.clear();
    for (const auto& update : bid_updates) {
        if (update.qty > 0) {
            bids[update.price] = update.qty;
        }
    }
    for (const auto& update : ask_updates) {
        if (update.qty > 0) {
            asks[update.price] = update.qty;
        }
    }
}

TopFive OrderBook::topFive() const {
    TopFive result{};
    size_t idx = 0;
    for (const auto& [price, qty] : bids) {
        if (idx >= 5) {
            break;
        }
        result.bid_prices[idx] = price;
        result.bid_sizes[idx] = qty;
        ++idx;
    }
    idx = 0;
    for (const auto& [price, qty] : asks) {
        if (idx >= 5) {
            break;
        }
        result.ask_prices[idx] = price;
        result.ask_sizes[idx] = qty;
        ++idx;
    }
    return result;
}

}  // namespace binance_capture
