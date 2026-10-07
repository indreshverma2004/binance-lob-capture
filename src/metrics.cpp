#include "metrics.hpp"

#include <string>

namespace binance_capture {

namespace {

bool isZeroQuantity(const nlohmann::json& value) {
    if (!value.is_string()) {
        return false;
    }
    const auto text = value.get<std::string>();
    bool found_digit = false;
    for (const char ch : text) {
        if (ch == '.') {
            continue;
        }
        if (ch < '0' || ch > '9') {
            return false;
        }
        found_digit = true;
        if (ch != '0') {
            return false;
        }
    }
    return found_digit;
}

uint64_t countZeroQuantityUpdates(const nlohmann::json& payload) {
    uint64_t count = 0;
    for (const auto* side : {"b", "a"}) {
        if (!payload.contains(side) || !payload[side].is_array()) {
            continue;
        }
        for (const auto& level : payload[side]) {
            if (level.is_array() && level.size() == 2 && isZeroQuantity(level[1])) {
                ++count;
            }
        }
    }
    return count;
}

}  // namespace

void MetricsCollector::observeEvent(const NormalizedMarketEvent& event, const ProcessedMarketEvent& result) {
    ++counters_.total_events;
    if (event.stream_kind == "trade") {
        ++counters_.trade_events;
    } else if (event.stream_kind == "depth_diff") {
        ++counters_.depth_diff_events;
        counters_.zero_quantity_updates += countZeroQuantityUpdates(event.payload);
    } else if (event.stream_kind == "depth5") {
        ++counters_.depth5_events;
    }

    if (result.disposition == EventDisposition::applied_diff) {
        ++counters_.depth_diffs_applied;
    } else if (result.disposition == EventDisposition::stale_diff) {
        ++counters_.stale_depth_diffs;
    }
    if (result.order_book_row) {
        ++counters_.order_book_rows;
    }
}

void MetricsCollector::recordRecoveredDepthEvents(size_t applied, size_t stale, size_t rows_emitted) {
    counters_.depth_diffs_applied += static_cast<uint64_t>(applied);
    counters_.stale_depth_diffs += static_cast<uint64_t>(stale);
    counters_.order_book_rows += static_cast<uint64_t>(rows_emitted);
}

void MetricsCollector::recordReconnect() noexcept {
    ++counters_.reconnects;
}

void MetricsCollector::recordRestResynchronization() noexcept {
    ++counters_.rest_resynchronizations;
}

void MetricsCollector::recordRestSnapshotFailure() noexcept {
    ++counters_.rest_snapshot_failures;
}

MetricsSnapshot MetricsCollector::snapshot() const noexcept {
    return counters_;
}

double MetricsCollector::ratePerSecond(uint64_t count, double elapsed_seconds) noexcept {
    if (elapsed_seconds <= 0.0) {
        return 0.0;
    }
    return static_cast<double>(count) / elapsed_seconds;
}

}  // namespace binance_capture
