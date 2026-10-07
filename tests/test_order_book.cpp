#include "binance_capture.hpp"

#include <cassert>
#include <iostream>
#include <limits>

int main() {
    binance_capture::OrderBook book;
    book.applyDepthDiff({{150000000LL, 100000000LL}, {151000000LL, 50000000LL}},
                        {{155000000LL, 120000000LL}, {154000000LL, 60000000LL}});
    assert(book.bids.size() == 2);
    assert(book.asks.size() == 2);

    const auto top = book.topFive();
    assert(top.bid_prices[0] == 151000000LL);
    assert(top.bid_sizes[0] == 50000000LL);
    assert(top.ask_prices[0] == 154000000LL);
    assert(top.ask_sizes[0] == 60000000LL);

    book.applyDepthDiff({{151000000LL, 0}}, {{154000000LL, 0}});
    assert(book.bids.size() == 1);
    assert(book.asks.size() == 1);
    assert(book.topFive().bid_prices[0] == 150000000LL);
    assert(book.topFive().ask_prices[0] == 155000000LL);

    book.applyDepth5({{152000000LL, 70000000LL}}, {{153000000LL, 80000000LL}});
    assert(book.bids.size() == 1);
    assert(book.asks.size() == 1);
    assert(book.topFive().bid_prices[0] == 152000000LL);
    assert(book.topFive().ask_prices[0] == 153000000LL);

    const std::string s = "a,b\"c\r\nd";
    const std::string csv = binance_capture::csvEscape(s);
    assert(csv == "\"a,b\"\"c\r\nd\"");

    assert(binance_capture::buildMarketDataHeader() ==
           "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json");
    assert(binance_capture::buildOrderBookHeader() ==
           "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4");
    assert(binance_capture::scaledIntegerFromString("83983.14000000", binance_capture::PRICE_SCALE, "price") == 8398314000000LL);
    assert(binance_capture::scaledIntegerFromString("0.00007000", binance_capture::QTY_SCALE, "qty") == 7000LL);
    assert(binance_capture::scaledIntegerFromString("-0.00000001", binance_capture::PRICE_SCALE, "price") == -1LL);
    assert(binance_capture::scaledIntegerFromString("1.230000000", binance_capture::PRICE_SCALE, "price") == 123000000LL);
    assert(binance_capture::scaledIntegerFromString("92233720368.54775807", binance_capture::PRICE_SCALE, "price") == std::numeric_limits<int64_t>::max());
    assert(binance_capture::scaledIntegerFromString("-92233720368.54775808", binance_capture::PRICE_SCALE, "price") == std::numeric_limits<int64_t>::min());

    bool overflow_detected = false;
    try {
        (void)binance_capture::scaledIntegerFromString("92233720368.54775808", binance_capture::PRICE_SCALE, "price");
    } catch (const std::overflow_error&) {
        overflow_detected = true;
    }
    assert(overflow_detected);

    bool excess_precision_rejected = false;
    try {
        (void)binance_capture::scaledIntegerFromString("1.000000001", binance_capture::PRICE_SCALE, "price");
    } catch (const std::invalid_argument&) {
        excess_precision_rejected = true;
    }
    assert(excess_precision_rejected);

    assert(binance_capture::buildStreamQuery("spot", "BTCUSDT") ==
           "wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms/btcusdt@depth5@100ms/btcusdt@trade");
    assert(binance_capture::buildStreamQuery("usdm", "BTCUSDT") ==
           "wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms/btcusdt@depth5@100ms/btcusdt@trade");
    assert(binance_capture::classifyStream("btcusdt@depth@100ms") == "depth_diff");
    assert(binance_capture::classifyStream("btcusdt@depth5@100ms") == "depth5");
    assert(binance_capture::classifyStream("btcusdt@trade") == "trade");

    uint64_t spot_update_id = binance_capture::depthSnapshotUpdateId("spot", {{"lastUpdateId", 100}});
    bool spot_initial_update_pending = true;
    assert(binance_capture::applyDepthSequence("spot", {{"U", 101}, {"u", 102}}, spot_update_id, spot_initial_update_pending) == binance_capture::DepthSequenceStatus::applied);
    assert(spot_update_id == 102);
    assert(binance_capture::applyDepthSequence("spot", {{"U", 102}, {"u", 104}}, spot_update_id, spot_initial_update_pending) == binance_capture::DepthSequenceStatus::applied);
    assert(spot_update_id == 104);
    assert(binance_capture::applyDepthSequence("spot", {{"U", 101}, {"u", 102}}, spot_update_id, spot_initial_update_pending) == binance_capture::DepthSequenceStatus::stale);
    assert(binance_capture::applyDepthSequence("spot", {{"U", 106}, {"u", 107}}, spot_update_id, spot_initial_update_pending) == binance_capture::DepthSequenceStatus::gap);

    uint64_t usdm_update_id = 200;
    assert(binance_capture::depthSnapshotUpdateId("usdm", {{"U", 200}, {"u", 205}, {"pu", 199}}) == 205);
    bool usdm_initial_update_pending = true;
    assert(binance_capture::applyDepthSequence("usdm", {{"U", 190}, {"u", 205}, {"pu", 189}}, usdm_update_id, usdm_initial_update_pending) == binance_capture::DepthSequenceStatus::applied);
    assert(usdm_update_id == 205);
    assert(!usdm_initial_update_pending);
    assert(binance_capture::applyDepthSequence("usdm", {{"U", 206}, {"u", 210}, {"pu", 205}}, usdm_update_id, usdm_initial_update_pending) == binance_capture::DepthSequenceStatus::applied);
    assert(usdm_update_id == 210);
    assert(binance_capture::applyDepthSequence("usdm", {{"U", 211}, {"u", 214}, {"pu", 210}}, usdm_update_id, usdm_initial_update_pending) == binance_capture::DepthSequenceStatus::applied);
    assert(usdm_update_id == 214);
    assert(binance_capture::applyDepthSequence("usdm", {{"U", 210}, {"u", 214}, {"pu", 205}}, usdm_update_id, usdm_initial_update_pending) == binance_capture::DepthSequenceStatus::stale);
    assert(binance_capture::applyDepthSequence("usdm", {{"U", 215}, {"u", 220}, {"pu", 209}}, usdm_update_id, usdm_initial_update_pending) == binance_capture::DepthSequenceStatus::gap);

    std::cout << "order_book_tests passed\n";
    return 0;
}
