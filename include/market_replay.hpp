#pragma once

#include "binance_capture.hpp"

#include <cstddef>
#include <cstdint>
#include <istream>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace binance_capture {

constexpr size_t MAX_BUFFERED_DEPTH_EVENTS = 2048;

enum class OrderBookRecoveryMode {
    depth5_snapshot,
    rest_snapshot
};

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
    buffered,
    buffer_overflow,
    depth5_observed,
    depth5_mismatch,
    trade
};

struct ProcessedMarketEvent {
    EventDisposition disposition = EventDisposition::trade;
    std::optional<OrderBookRow> order_book_row;
};

struct SnapshotRecoveryResult;

class OrderBookProcessor {
public:
    OrderBookProcessor(std::string venue,
                       std::string symbol,
                       OrderBookRecoveryMode recovery_mode = OrderBookRecoveryMode::depth5_snapshot,
                       size_t buffer_limit = MAX_BUFFERED_DEPTH_EVENTS);

    ProcessedMarketEvent process(const NormalizedMarketEvent& event);
    void reset();
    void beginResynchronization();
    bool isResynchronizing() const;
    size_t bufferedDepthEventCount() const;
    SnapshotRecoveryResult restoreFromRestSnapshot(const nlohmann::json& snapshot);

private:
    std::string venue_;
    std::string symbol_;
    OrderBook order_book_;
    uint64_t orderbook_seq_ = 0;
    uint64_t last_depth_seq_ = 0;
    bool have_depth_baseline_ = false;
    bool initial_depth_diff_pending_ = false;
    bool book_out_of_sync_ = true;
    OrderBookRecoveryMode recovery_mode_ = OrderBookRecoveryMode::depth5_snapshot;
    size_t buffer_limit_ = MAX_BUFFERED_DEPTH_EVENTS;
    bool resynchronizing_ = false;
    std::vector<NormalizedMarketEvent> buffered_depth_events_;
};

struct RestDepthSnapshot {
    uint64_t last_update_id = 0;
    std::vector<DepthLevel> bids;
    std::vector<DepthLevel> asks;
};

struct SnapshotRecoveryResult {
    bool synchronized = false;
    uint64_t snapshot_update_id = 0;
    size_t buffered_events = 0;
    size_t stale_events_discarded = 0;
    std::string reason;
    std::vector<OrderBookRow> rows;
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
RestDepthSnapshot parseRestDepthSnapshot(const std::string& venue, const nlohmann::json& payload);
uint32_t restRetryDelaySeconds(uint32_t failed_attempt, uint32_t retry_after_seconds = 0);
ReplayStats replayMarketDataCsv(std::istream& input, std::ostream& order_book_output);

}  // namespace binance_capture