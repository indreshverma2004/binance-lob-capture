#include "market_replay.hpp"
#include "rest_snapshot_client.hpp"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using binance_capture::DepthLevel;
using binance_capture::EventDisposition;
using binance_capture::NormalizedMarketEvent;
using binance_capture::OrderBookProcessor;
using binance_capture::OrderBookRecoveryMode;
using binance_capture::SnapshotRecoveryResult;

nlohmann::json snapshot(uint64_t update_id,
                        const std::vector<std::pair<std::string, std::string>>& bids,
                        const std::vector<std::pair<std::string, std::string>>& asks) {
    nlohmann::json result;
    result["lastUpdateId"] = update_id;
    result["bids"] = nlohmann::json::array();
    result["asks"] = nlohmann::json::array();
    for (const auto& level : bids) result["bids"].push_back({level.first, level.second});
    for (const auto& level : asks) result["asks"].push_back({level.first, level.second});
    return result;
}

NormalizedMarketEvent depthEvent(const std::string& venue,
                                 uint64_t conn_seq,
                                 uint64_t first,
                                 uint64_t final,
                                 uint64_t previous,
                                 const nlohmann::json& bids,
                                 const nlohmann::json& asks,
                                 int64_t tsec) {
    nlohmann::json payload = {{"U", first}, {"u", final}, {"b", bids}, {"a", asks}};
    if (venue == "usdm") payload["pu"] = previous;
    return {tsec, static_cast<int32_t>(conn_seq), venue, "depth_diff", 0, 0, conn_seq, "BTCUSDT", payload};
}

nlohmann::json levels(std::initializer_list<std::pair<std::string, std::string>> values) {
    auto result = nlohmann::json::array();
    for (const auto& value : values) result.push_back({value.first, value.second});
    return result;
}

void testRestSnapshotParsing() {
    const auto body = snapshot(501, {{"100.25", "2.5"}}, {{"100.50", "3.5"}});
    const auto spot = binance_capture::parseRestDepthSnapshot("spot", body);
    const auto usdm = binance_capture::parseRestDepthSnapshot("usdm", body);
    assert(spot.last_update_id == 501);
    assert(usdm.last_update_id == 501);
    assert(spot.bids.size() == 1 && spot.bids[0].price == 10025000000LL && spot.bids[0].qty == 250000000LL);
    assert(spot.asks.size() == 1 && spot.asks[0].price == 10050000000LL && spot.asks[0].qty == 350000000LL);

    auto expectFailure = [](const std::string& venue, const nlohmann::json& payload) {
        try {
            (void)binance_capture::parseRestDepthSnapshot(venue, payload);
        } catch (const std::exception&) {
            return;
        }
        assert(false && "expected REST snapshot parse failure");
    };
    expectFailure("spot", {{"bids", nlohmann::json::array()}, {"asks", nlohmann::json::array()}});
    expectFailure("spot", {{"lastUpdateId", 1}, {"bids", "bad"}, {"asks", nlohmann::json::array()}});
    expectFailure("spot", {{"lastUpdateId", 1}, {"bids", nlohmann::json::array()}});
    expectFailure("usdm", {{"lastUpdateId", 1}, {"bids", {{"bad", "1"}}}, {"asks", nlohmann::json::array()}});
    expectFailure("usdm", {{"lastUpdateId", 1}, {"bids", {{"1", "1.000000001"}}}, {"asks", nlohmann::json::array()}});
    expectFailure("spot", {{"lastUpdateId", 1}, {"bids", {{"-1", "1"}}}, {"asks", nlohmann::json::array()}});
    expectFailure("usdm", {{"lastUpdateId", 1}, {"bids", {{"1", "-1"}}}, {"asks", nlohmann::json::array()}});
}

void testSpotBufferedRecoveryAndRetry() {
    OrderBookProcessor processor("spot", "BTCUSDT", OrderBookRecoveryMode::rest_snapshot);
    processor.beginResynchronization();

    const NormalizedMarketEvent stale = depthEvent("spot", 1, 90, 100, 0,
        levels({{"10.00", "0"}}), levels({}), 10);
    const NormalizedMarketEvent first = depthEvent("spot", 2, 100, 102, 0,
        levels({{"10.00", "0"}, {"9.00", "4.00"}}), levels({{"11.00", "3.50"}}), 11);
    const NormalizedMarketEvent second = depthEvent("spot", 3, 103, 104, 0,
        levels({{"8.00", "1.00"}}), levels({}), 12);
    assert(processor.process(stale).disposition == EventDisposition::buffered);
    assert(processor.process(first).disposition == EventDisposition::buffered);
    assert(processor.process(second).disposition == EventDisposition::buffered);

    try {
        (void)processor.restoreFromRestSnapshot({{"bids", nlohmann::json::array()}, {"asks", nlohmann::json::array()}});
        assert(false && "snapshot without lastUpdateId must fail");
    } catch (const std::invalid_argument&) {
        assert(processor.isResynchronizing());
        assert(processor.bufferedDepthEventCount() == 3);
    }

    const auto recovered = processor.restoreFromRestSnapshot(snapshot(100,
        {{"10.00", "2.00"}}, {{"11.00", "3.00"}}));
    assert(recovered.synchronized);
    assert(recovered.stale_events_discarded == 1);
    assert(recovered.rows.size() == 2);
    assert(recovered.rows[0].tsec == 11 && recovered.rows[0].tnsec == 2);
    assert(recovered.rows[0].type == 'D' && recovered.rows[0].seqNo == 1);
    assert(recovered.rows[0].bid_prices[0] == 900000000LL);
    assert(recovered.rows[0].bid_sizes[0] == 400000000LL);
    assert(recovered.rows[0].ask_sizes[0] == 350000000LL);
    assert(recovered.rows[1].tsec == 12 && recovered.rows[1].bid_prices[0] == 900000000LL);
    assert(recovered.rows[1].bid_prices[1] == 800000000LL);

    const auto gap = depthEvent("spot", 4, 106, 107, 0, levels({}), levels({}), 13);
    const auto after_gap = depthEvent("spot", 5, 108, 109, 0,
        levels({{"7.00", "2.00"}}), levels({}), 14);
    assert(processor.process(gap).disposition == EventDisposition::gap);
    assert(processor.process(after_gap).disposition == EventDisposition::buffered);
    assert(processor.isResynchronizing());

    const auto missed_bridge = processor.restoreFromRestSnapshot(snapshot(100,
        {{"10.00", "1.00"}}, {{"11.00", "1.00"}}));
    assert(!missed_bridge.synchronized);
    assert(missed_bridge.rows.empty());
    assert(processor.isResynchronizing());
    assert(processor.bufferedDepthEventCount() == 2);

    const auto retried = processor.restoreFromRestSnapshot(snapshot(105,
        {{"12.00", "1.00"}}, {{"13.00", "1.00"}}));
    assert(retried.synchronized);
    assert(retried.rows.size() == 2);
    assert(retried.rows[0].seqNo == 3 && retried.rows[0].tsec == 13);
    assert(retried.rows[1].seqNo == 4 && retried.rows[1].tsec == 14);
    assert(retried.rows[1].bid_prices[0] == 1200000000LL);
    assert(retried.rows[1].bid_prices[1] == 700000000LL);
    assert(retried.rows[1].bid_prices[2] == 0);
}

void testUsdmPuRecoveryAndReconnectReset() {
    OrderBookProcessor processor("usdm", "BTCUSDT", OrderBookRecoveryMode::rest_snapshot);
    processor.beginResynchronization();
    assert(processor.process(depthEvent("usdm", 1, 198, 202, 197,
        levels({{"10.00", "1.00"}}), levels({}), 20)).disposition == EventDisposition::buffered);
    assert(processor.process(depthEvent("usdm", 2, 203, 204, 202,
        levels({{"11.00", "2.00"}}), levels({}), 21)).disposition == EventDisposition::buffered);

    const auto initial = processor.restoreFromRestSnapshot(snapshot(200,
        {{"9.00", "1.00"}}, {{"12.00", "1.00"}}));
    assert(initial.synchronized && initial.rows.size() == 2);
    assert(initial.rows[0].bid_prices[0] == 1000000000LL);
    assert(initial.rows[1].bid_prices[0] == 1100000000LL);

    const auto mismatch = depthEvent("usdm", 3, 205, 206, 201,
        levels({{"13.00", "1.00"}}), levels({}), 22);
    assert(processor.process(mismatch).disposition == EventDisposition::gap);
    assert(processor.isResynchronizing());
    assert(processor.process(depthEvent("usdm", 4, 207, 208, 206,
        levels({{"14.00", "1.00"}}), levels({}), 23)).disposition == EventDisposition::buffered);

    const auto recovered = processor.restoreFromRestSnapshot(snapshot(204,
        {{"12.50", "1.00"}}, {{"15.00", "1.00"}}));
    assert(recovered.synchronized && recovered.rows.size() == 2);
    assert(recovered.rows[0].seqNo == 3 && recovered.rows[0].tsec == 22);
    assert(recovered.rows[0].bid_prices[0] == 1300000000LL);
    assert(recovered.rows[1].seqNo == 4 && recovered.rows[1].bid_prices[0] == 1400000000LL);

    processor.beginResynchronization();
    assert(processor.process(depthEvent("usdm", 1, 1, 2, 0,
        levels({{"99.00", "99.00"}}), levels({}), 24)).disposition == EventDisposition::buffered);
    assert(processor.process(depthEvent("usdm", 2, 301, 301, 300,
        levels({{"20.00", "0.00"}}), levels({}), 25)).disposition == EventDisposition::buffered);
    const auto new_epoch = processor.restoreFromRestSnapshot(snapshot(300,
        {{"20.00", "2.00"}}, {{"21.00", "2.00"}}));
    assert(new_epoch.synchronized);
    assert(new_epoch.rows.size() == 1);
    assert(new_epoch.rows[0].seqNo == 5);
    assert(new_epoch.rows[0].bid_prices[0] == 0);
    assert(!processor.isResynchronizing());
}

void testDepth5IsNotRestAuthorityAndBufferOverflow() {
    OrderBookProcessor processor("spot", "BTCUSDT", OrderBookRecoveryMode::rest_snapshot, 2);
    processor.beginResynchronization();
    NormalizedMarketEvent partial{1, 1, "spot", "depth5", 0, 0, 1, "BTCUSDT",
        {{"lastUpdateId", 10}, {"bids", levels({{"99.00", "1.00"}})}, {"asks", levels({{"101.00", "1.00"}})}}};
    assert(processor.process(partial).disposition == EventDisposition::depth5_observed);
    assert(processor.bufferedDepthEventCount() == 0);

    assert(processor.process(depthEvent("spot", 2, 1, 1, 0, levels({}), levels({}), 2)).disposition == EventDisposition::buffered);
    assert(processor.process(depthEvent("spot", 3, 2, 2, 0, levels({}), levels({}), 3)).disposition == EventDisposition::buffered);
    assert(processor.process(depthEvent("spot", 4, 3, 3, 0, levels({}), levels({}), 4)).disposition == EventDisposition::buffer_overflow);
    assert(processor.isResynchronizing());
    assert(processor.bufferedDepthEventCount() == 0);
    const auto after_overflow = processor.restoreFromRestSnapshot(snapshot(3,
        {{"50.00", "1.00"}}, {{"51.00", "1.00"}}));
    assert(after_overflow.synchronized);
    assert(after_overflow.rows.empty());
}

void testDepth5SanityDoesNotReplaceRestBook() {
    OrderBookProcessor processor("spot", "BTCUSDT", OrderBookRecoveryMode::rest_snapshot);
    processor.beginResynchronization();
    const auto initial = processor.restoreFromRestSnapshot(snapshot(500,
        {{"10.00", "2.00"}, {"9.00", "3.00"}}, {{"11.00", "4.00"}}));
    assert(initial.synchronized);

    NormalizedMarketEvent matching_partial{30, 1, "spot", "depth5", 0, 0, 1, "BTCUSDT",
        {{"lastUpdateId", 500}, {"bids", levels({{"10.00", "2.00"}})},
         {"asks", levels({{"11.00", "4.00"}})}}};
    const auto observed = processor.process(matching_partial);
    assert(observed.disposition == EventDisposition::depth5_observed);
    assert(observed.order_book_row.has_value());
    assert(observed.order_book_row->type == 'S');
    assert(observed.order_book_row->tsec == 30);

    NormalizedMarketEvent mismatch{31, 2, "spot", "depth5", 0, 0, 2, "BTCUSDT",
        {{"lastUpdateId", 501}, {"bids", levels({{"8.00", "1.00"}})},
         {"asks", levels({{"11.00", "4.00"}})}}};
    const auto failed_sanity = processor.process(mismatch);
    assert(failed_sanity.disposition == EventDisposition::depth5_mismatch);
    assert(!failed_sanity.order_book_row.has_value());
    assert(!processor.isResynchronizing());
    assert(processor.bufferedDepthEventCount() == 0);
}

void testMockedRestTransportAndFailures() {
    const auto spot_request = binance_capture::buildRestDepthRequest("spot", "btcusdt");
    assert(spot_request.host == "api.binance.com");
    assert(spot_request.target == "/api/v3/depth?symbol=BTCUSDT&limit=1000");
    const auto usdm_request = binance_capture::buildRestDepthRequest("usdm", "btcusdt");
    assert(usdm_request.host == "fapi.binance.com");
    assert(usdm_request.target == "/fapi/v1/depth?symbol=BTCUSDT&limit=1000");

    bool transport_called = false;
    const auto payload = binance_capture::fetchRestDepthSnapshot("spot", "btcusdt",
        [&transport_called](const binance_capture::RestDepthRequest& request) {
            transport_called = true;
            assert(request.host == "api.binance.com");
            return binance_capture::RestHttpResponse{
                200,
                R"({"lastUpdateId":501,"bids":[["100.25","2.5"]],"asks":[["100.50","3.5"]]})",
                0
            };
        });
    assert(transport_called);
    assert(payload["lastUpdateId"] == 501);

    auto expectRestFailure = [](const binance_capture::RestHttpTransport& transport,
                                uint32_t expected_retry_after,
                                const std::string& message_fragment) {
        try {
            (void)binance_capture::fetchRestDepthSnapshot("usdm", "BTCUSDT", transport);
        } catch (const binance_capture::RestRequestError& exc) {
            assert(exc.retryAfterSeconds() == expected_retry_after);
            assert(std::string(exc.what()).find(message_fragment) != std::string::npos);
            return;
        }
        assert(false && "expected REST request failure");
    };
    expectRestFailure([](const auto&) {
        return binance_capture::RestHttpResponse{429, "rate limited", 17};
    }, 17, "HTTP 429");
    expectRestFailure([](const auto&) {
        return binance_capture::RestHttpResponse{418, "IP banned", 60};
    }, 60, "HTTP 418");
    expectRestFailure([](const auto&) {
        return binance_capture::RestHttpResponse{200, "{bad json", 0};
    }, 0, "malformed REST snapshot response");
    expectRestFailure([](const auto&) {
        return binance_capture::RestHttpResponse{200,
            R"({"lastUpdateId":1,"bids":[["bad","1"]],"asks":[]})", 0};
    }, 0, "invalid integer part");
    expectRestFailure([](const auto&) -> binance_capture::RestHttpResponse {
        throw std::runtime_error("simulated TLS/connect failure");
    }, 0, "simulated TLS/connect failure");

    uint32_t attempts = 0;
    uint32_t retry_delay = 0;
    nlohmann::json retried_payload;
    while (attempts < 3 && retried_payload.is_null()) {
        try {
            retried_payload = binance_capture::fetchRestDepthSnapshot("spot", "BTCUSDT",
                [&attempts](const binance_capture::RestDepthRequest&) {
                    ++attempts;
                    if (attempts == 1) {
                        return binance_capture::RestHttpResponse{429, "rate limited", 7};
                    }
                    return binance_capture::RestHttpResponse{200,
                        R"({"lastUpdateId":700,"bids":[],"asks":[]})", 0};
                });
        } catch (const binance_capture::RestRequestError& exc) {
            retry_delay = binance_capture::restRetryDelaySeconds(attempts, exc.retryAfterSeconds());
        }
    }
    assert(attempts == 2);
    assert(retry_delay == 7);
    assert(retried_payload["lastUpdateId"] == 700);
}

}  // namespace

int main() {
    try {
        testRestSnapshotParsing();
    } catch (const std::exception& exc) {
        std::cerr << "testRestSnapshotParsing: " << exc.what() << '\n';
        throw;
    }
    try {
        testSpotBufferedRecoveryAndRetry();
    } catch (const std::exception& exc) {
        std::cerr << "testSpotBufferedRecoveryAndRetry: " << exc.what() << '\n';
        throw;
    }
    try {
        testUsdmPuRecoveryAndReconnectReset();
    } catch (const std::exception& exc) {
        std::cerr << "testUsdmPuRecoveryAndReconnectReset: " << exc.what() << '\n';
        throw;
    }
    try {
        testDepth5IsNotRestAuthorityAndBufferOverflow();
    } catch (const std::exception& exc) {
        std::cerr << "testDepth5IsNotRestAuthorityAndBufferOverflow: " << exc.what() << '\n';
        throw;
    }
    testDepth5SanityDoesNotReplaceRestBook();
    testMockedRestTransportAndFailures();
    assert(binance_capture::restRetryDelaySeconds(1) == 2);
    assert(binance_capture::restRetryDelaySeconds(2) == 4);
    assert(binance_capture::restRetryDelaySeconds(5) == 30);
    assert(binance_capture::restRetryDelaySeconds(1, 45) == 45);
    std::cout << "resync_tests passed\n";
    return 0;
}