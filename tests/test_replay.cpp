#include "market_replay.hpp"

#include <cassert>
#include <iostream>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::string validMarketCsv() {
    return "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
           R"(100,123,spot,depth5,0,0,1,BTCUSDT,"{""lastUpdateId"":100,""bids"":[[""10.00000000"",""2.00000000""]],""asks"":[[""11.00000000"",""3.00000000""]]}")" "\n"
           R"(101,456,spot,depth_diff,0,0,2,BTCUSDT,"{""U"":101,""u"":101,""b"":[[""10.00000000"",""0.00000000""],[""9.00000000"",""4.00000000""]],""a"":[[""11.00000000"",""3.50000000""]]}")" "\n"
           R"(102,789,spot,trade,0,0,3,BTCUSDT,"{""e"":""trade"",""p"":""9.00000000"",""q"":""7.00000000""}")" "\n"
           R"(103,987,spot,depth_diff,0,0,4,BTCUSDT,"{""U"":102,""u"":102,""b"":[],""a"":[]}")" "\n"
           R"(104,987654321,spot,depth5,0,0,5,BTCUSDT,"{""lastUpdateId"":110,""bids"":[[""12.00000000"",""1.00000000""]],""asks"":[[""13.00000000"",""2.00000000""]]}")" "\n";
}

std::vector<std::string> csvFields(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream input(line);
    std::string field;
    while (std::getline(input, field, ',')) {
        fields.push_back(field);
    }
    return fields;
}

std::string runReplay(const std::string& input, binance_capture::ReplayStats* stats = nullptr) {
    std::istringstream source(input);
    std::ostringstream destination;
    const auto result = binance_capture::replayMarketDataCsv(source, destination);
    if (stats != nullptr) {
        *stats = result;
    }
    return destination.str();
}

void expectReplayFailure(const std::string& input, const std::string& expected_message) {
    try {
        (void)runReplay(input);
    } catch (const std::exception& exc) {
        if (std::string(exc.what()).find(expected_message) == std::string::npos) {
            std::cerr << "Expected replay error containing: " << expected_message
                      << "; received: " << exc.what() << '\n';
            assert(false && "unexpected replay error");
        }
        return;
    }
    std::cerr << "Expected replay error containing: " << expected_message << "; input was accepted\n";
    assert(false && "expected replay to reject malformed input");
}

}  // namespace

int main() {
    const std::string input = validMarketCsv();
    binance_capture::ReplayStats stats;
    const std::string first_output = runReplay(input, &stats);
    const std::string second_output = runReplay(input);

    assert(first_output == second_output);
    assert(stats.market_events == 5);
    assert(stats.depth5_events == 2);
    assert(stats.depth_diff_events == 2);
    assert(stats.trade_events == 1);
    assert(stats.applied_diff_events == 2);
    assert(stats.stale_diff_events == 0);
    assert(stats.gap_events == 0);
    assert(stats.order_book_rows == 4);

    std::istringstream output_stream(first_output);
    std::string line;
    std::vector<std::vector<std::string>> rows;
    while (std::getline(output_stream, line)) {
        rows.push_back(csvFields(line));
    }
    assert(rows.size() == 5);
    assert(rows[0].size() == 26);
    assert(rows[1].size() == 26);
    assert(rows[2].size() == 26);
    assert(rows[3].size() == 26);
    assert(rows[0][0] == "tsec");
    assert(rows[0][1] == "tnsec");
    assert(rows[1][0] == "100");
    assert(rows[1][1] == "123");
    assert(rows[1][4] == "S");
    assert(rows[1][6] == "1000000000");
    assert(rows[1][16] == "1100000000");
    assert(rows[2][0] == "101");
    assert(rows[2][1] == "456");
    assert(rows[2][4] == "D");
    assert(rows[2][6] == "900000000");
    assert(rows[2][11] == "400000000");
    assert(rows[2][16] == "1100000000");
    assert(rows[2][21] == "350000000");
    assert(rows[3][0] == "103");
    assert(rows[3][1] == "987");
    assert(rows[3][4] == "D");
    assert(rows[3][6] == "900000000");
    assert(rows[3][11] == "400000000");
    assert(rows[3][21] == "350000000");
    assert(rows[4][0] == "104");
    assert(rows[4][1] == "987654321");
    assert(rows[4][4] == "S");
    assert(rows[4][6] == "1200000000");
    assert(rows[4][16] == "1300000000");

    const std::string epoch_input =
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "100,1,spot,depth5,0,0,1,BTCUSDT,\"{\"\"lastUpdateId\"\":100,\"\"bids\"\":[[\"\"10.00000000\"\",\"\"1.00000000\"\"]],\"\"asks\"\":[[\"\"11.00000000\"\",\"\"1.00000000\"\"]]}\"\n"
        "101,2,spot,depth5,0,1,1,BTCUSDT,\"{\"\"lastUpdateId\"\":200,\"\"bids\"\":[[\"\"12.00000000\"\",\"\"1.00000000\"\"]],\"\"asks\"\":[[\"\"13.00000000\"\",\"\"1.00000000\"\"]]}\"\n";
    std::istringstream epoch_output_stream(runReplay(epoch_input));
    std::getline(epoch_output_stream, line);
    std::getline(epoch_output_stream, line);
    assert(csvFields(line)[2] == "1");
    std::getline(epoch_output_stream, line);
    assert(csvFields(line)[2] == "2");

    expectReplayFailure("", "incorrect 9-column header");
    expectReplayFailure("wrong,header\n", "incorrect 9-column header");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,2,spot,trade,0,0,1,BTCUSDT,\"{\"\"e\"\":\"\"trade\"\"}\n",
        "unterminated quoted CSV field");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,2,spot,trade,0,0,1,BTCUSDT,\"{not-json}\"\n",
        "malformed payload_json");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,1000000000,spot,trade,0,0,1,BTCUSDT,{}\n",
        "recv_tnsec outside");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,1,spot,trade,0,0,1,BTCUSDT,{},extra\n",
        "expected 9 columns");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,1,spot,unknown,0,0,1,BTCUSDT,{}\n",
        "unsupported stream_kind");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,1,spot,depth_diff,0,0,1,BTCUSDT," + binance_capture::csvEscape("{\"b\":[],\"a\":[]}") + "\n",
        "row 2: depth event missing sequence field U");
    expectReplayFailure(
        "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n"
        "1,1,spot,depth5,0,0,1,BTCUSDT," +
            binance_capture::csvEscape("{\"lastUpdateId\":1,\"bids\":[[\"bad\",\"1.0\"]],\"asks\":[]}") + "\n",
        "invalid integer part");

    std::cout << "market_replay_tests passed\n";
    return 0;
}