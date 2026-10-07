#pragma once

#include "market_replay.hpp"

#include <cstdint>

namespace binance_capture {

struct MetricsSnapshot {
    uint64_t total_events = 0;
    uint64_t trade_events = 0;
    uint64_t depth_diff_events = 0;
    uint64_t depth5_events = 0;
    uint64_t order_book_rows = 0;
    uint64_t depth_diffs_applied = 0;
    uint64_t stale_depth_diffs = 0;
    uint64_t zero_quantity_updates = 0;
    uint64_t reconnects = 0;
    uint64_t rest_resynchronizations = 0;
    uint64_t rest_snapshot_failures = 0;
};

class MetricsCollector {
public:
    void observeEvent(const NormalizedMarketEvent& event, const ProcessedMarketEvent& result);
    void recordRecoveredDepthEvents(size_t applied, size_t stale, size_t rows_emitted);
    void recordReconnect() noexcept;
    void recordRestResynchronization() noexcept;
    void recordRestSnapshotFailure() noexcept;

    MetricsSnapshot snapshot() const noexcept;
    static double ratePerSecond(uint64_t count, double elapsed_seconds) noexcept;

private:
    MetricsSnapshot counters_;
};

}  // namespace binance_capture
