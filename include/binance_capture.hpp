#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace binance_capture {

constexpr int64_t PRICE_SCALE = 100000000LL;
constexpr int64_t QTY_SCALE = 100000000LL;

enum class DepthSequenceStatus {
    applied,
    stale,
    gap
};

struct DepthLevel {
    int64_t price = 0;
    int64_t qty = 0;
};

struct TopFive {
    std::array<int64_t, 5> bid_prices{};
    std::array<int64_t, 5> bid_sizes{};
    std::array<int64_t, 5> ask_prices{};
    std::array<int64_t, 5> ask_sizes{};
};

struct MarketDataRow {
    int64_t recv_tsec = 0;
    int32_t recv_tnsec = 0;
    std::string venue;
    std::string stream_kind;
    int64_t shard_id = 0;
    int64_t conn_epoch = 0;
    uint64_t conn_seq = 0;
    std::string symbol;
    std::string payload_json;
};

struct OrderBookRow {
    int64_t tsec = 0;
    int32_t tnsec = 0;
    uint64_t seqNo = 0;
    int32_t id = 0;
    char type = 'D';
    char side = 'N';
    std::array<int64_t, 5> bid_prices{};
    std::array<int64_t, 5> bid_sizes{};
    std::array<int64_t, 5> ask_prices{};
    std::array<int64_t, 5> ask_sizes{};
};

std::string csvEscape(const std::string& value);
std::string normalizeSymbol(const std::string& symbol);
std::string buildStreamQuery(const std::string& venue, const std::string& symbol);
std::string buildMarketDataHeader();
std::string buildOrderBookHeader();
std::string compactJson(const nlohmann::json& value);
int64_t scaledIntegerFromString(const std::string& text, int64_t scale, const std::string& label);
uint32_t stableInstrumentId(const std::string& symbol);
uint64_t depthSnapshotUpdateId(const std::string& venue, const nlohmann::json& payload);
DepthSequenceStatus applyDepthSequence(const std::string& venue,
                                       const nlohmann::json& payload,
                                       uint64_t& last_update_id,
                                       bool& initial_update_pending);

class OrderBook {
public:
    std::map<int64_t, int64_t, std::greater<int64_t>> bids;
    std::map<int64_t, int64_t, std::less<int64_t>> asks;
    bool out_of_sync = false;

    void applyDepthDiff(const std::vector<DepthLevel>& bid_updates,
                        const std::vector<DepthLevel>& ask_updates);
    void applyDepth5(const std::vector<DepthLevel>& bid_updates,
                     const std::vector<DepthLevel>& ask_updates);
    TopFive topFive() const;
};

std::string classifyStream(const std::string& stream_name);

}  // namespace binance_capture
