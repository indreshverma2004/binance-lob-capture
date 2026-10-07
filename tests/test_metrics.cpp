#include "metrics.hpp"

#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string>

namespace {

std::string benchmarkFixture() {
    return "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
           "100,123,spot,depth5,0,0,1,BTCUSDT,\"{\"\"lastUpdateId\"\":100,\"\"bids\"\":[[\"\"10.00000000\"\",\"\"2.00000000\"\"]],\"\"asks\"\":[[\"\"11.00000000\"\",\"\"3.00000000\"\"]]}\"\n"
           "101,456,spot,depth_diff,0,0,2,BTCUSDT,\"{\"\"U\"\":101,\"\"u\"\":101,\"\"b\"\":[[\"\"10.00000000\"\",\"\"0.00000000\"\"]],\"\"a\"\":[]}\"\n"
           "102,789,spot,trade,0,0,3,BTCUSDT,\"{\"\"e\"\":\"\"trade\"\"}\"\n";
}

void testCountersAndRates() {
    binance_capture::MetricsCollector metrics;
    const binance_capture::NormalizedMarketEvent depth5{
        1, 0, "spot", "depth5", 0, 0, 1, "BTCUSDT",
        {{"lastUpdateId", 10}, {"bids", nlohmann::json::array()}, {"asks", nlohmann::json::array()}}
    };
    nlohmann::json diff_payload = {{"U", 11}, {"u", 11}, {"b", nlohmann::json::array()},
                                  {"a", nlohmann::json::array()}};
    diff_payload["b"].push_back({"10.0", "0.00000000"});
    const binance_capture::NormalizedMarketEvent diff{
        2, 0, "spot", "depth_diff", 0, 0, 2, "BTCUSDT", diff_payload
    };
    const binance_capture::NormalizedMarketEvent trade{
        3, 0, "spot", "trade", 0, 0, 3, "BTCUSDT", {{"e", "trade"}}
    };
    metrics.observeEvent(depth5, {binance_capture::EventDisposition::snapshot, std::nullopt});
    metrics.observeEvent(diff, {binance_capture::EventDisposition::applied_diff, std::nullopt});
    metrics.observeEvent(diff, {binance_capture::EventDisposition::stale_diff, std::nullopt});
    metrics.observeEvent(trade, {binance_capture::EventDisposition::trade, std::nullopt});
    metrics.recordRecoveredDepthEvents(2, 1, 2);
    metrics.recordReconnect();
    metrics.recordRestResynchronization();
    metrics.recordRestSnapshotFailure();

    const auto counts = metrics.snapshot();
    assert(counts.total_events == 4);
    assert(counts.trade_events == 1);
    assert(counts.depth_diff_events == 2);
    assert(counts.depth5_events == 1);
    assert(counts.order_book_rows == 2);
    assert(counts.depth_diffs_applied == 3);
    assert(counts.stale_depth_diffs == 2);
    assert(counts.zero_quantity_updates == 2);
    assert(counts.reconnects == 1);
    assert(counts.rest_resynchronizations == 1);
    assert(counts.rest_snapshot_failures == 1);

    assert(binance_capture::MetricsCollector::ratePerSecond(10, 0.0) == 0.0);
    assert(binance_capture::MetricsCollector::ratePerSecond(10, -1.0) == 0.0);
    const auto rate = binance_capture::MetricsCollector::ratePerSecond(10, 2.0);
    assert(rate == 5.0 && std::isfinite(rate) && rate >= 0.0);
}

void testReplayMetricsDoNotChangeOutput() {
    const auto input_text = benchmarkFixture();
    std::istringstream normal_input(input_text);
    std::istringstream measured_input(input_text);
    std::ostringstream normal_output;
    std::ostringstream measured_output;
    binance_capture::MetricsCollector metrics;

    const auto normal_stats = binance_capture::replayMarketDataCsv(normal_input, normal_output);
    const auto measured_stats = binance_capture::replayMarketDataCsv(measured_input, measured_output, &metrics);
    assert(normal_output.str() == measured_output.str());
    assert(normal_stats.market_events == measured_stats.market_events);
    assert(normal_stats.order_book_rows == measured_stats.order_book_rows);
    assert(measured_stats.market_events == 3);
    assert(measured_stats.order_book_rows == 2);
    const auto counts = metrics.snapshot();
    assert(counts.total_events == 3);
    assert(counts.order_book_rows == 2);
    assert(counts.depth_diffs_applied == 1);
    assert(counts.zero_quantity_updates == 1);

    const auto elapsed_seconds = 2.0;
    assert(binance_capture::MetricsCollector::ratePerSecond(counts.total_events, elapsed_seconds) == 1.5);
    assert(binance_capture::MetricsCollector::ratePerSecond(counts.order_book_rows, elapsed_seconds) == 1.0);
}

}  // namespace

int main() {
    testCountersAndRates();
    testReplayMetricsDoNotChangeOutput();
    std::cout << "metrics_tests passed\n";
    return 0;
}
