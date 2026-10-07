#pragma once

#include "binance_capture.hpp"

#include <cstdint>
#include <istream>
#include <optional>
#include <ostream>
#include <string>

namespace binance_capture {

struct NormalizedMarketEvent {
    int64_t recv_tsec = 0;
    int32_t recv_tnsec = 0;
    std::string venue;
    std::string stream_kind;
    int64_t shard_id = 0;
    int64_t conn_epoch = 0;
    uint64_t conn_seq = 0;
    std::string symbol;
    nlohmann::json payload;
};

enum class EventDisposition {
    snapshot,
    applied_diff,
    stale_diff,
    gap,
    ignored_out_of_sync,
    trade
};

struct ProcessedMarketEvent {
    EventDisposition disposition = EventDisposition::trade;
    std::optional<OrderBookRow> order_book_row;
};

class OrderBookProcessor {
public:
    OrderBookProcessor(std::string venue, std::string symbol);

    ProcessedMarketEvent process(const NormalizedMarketEvent& event);
    void reset();

private:
    std::string venue_;
    std::string symbol_;
    OrderBook order_book_;
    uint64_t orderbook_seq_ = 0;
    uint64_t last_depth_seq_ = 0;
    bool have_depth_baseline_ = false;
    bool initial_depth_diff_pending_ = false;
    bool book_out_of_sync_ = true;
};

struct ReplayStats {
    uint64_t market_events = 0;
    uint64_t depth_diff_events = 0;
    uint64_t depth5_events = 0;
    uint64_t trade_events = 0;
    uint64_t applied_diff_events = 0;
    uint64_t stale_diff_events = 0;
    uint64_t gap_events = 0;
    uint64_t ignored_out_of_sync_events = 0;
    uint64_t order_book_rows = 0;
};

void writeOrderBookCsvHeader(std::ostream& output);
void writeOrderBookCsvRow(std::ostream& output, const OrderBookRow& row);
ReplayStats replayMarketDataCsv(std::istream& input, std::ostream& order_book_output);

}  // namespace binance_capture