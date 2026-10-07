#include "binance_capture.hpp"

#include <cassert>
#include <iostream>

int main() {
    binance_capture::OrderBook book;
    book.applyDepthDiff({{150000000LL, 100000000LL}}, {{155000000LL, 120000000LL}});
    assert(book.bids.size() == 1);
    assert(book.asks.size() == 1);

    const auto top = book.topFive();
    assert(top.bid_prices[0] == 150000000LL);
    assert(top.bid_sizes[0] == 100000000LL);
    assert(top.ask_prices[0] == 155000000LL);
    assert(top.ask_sizes[0] == 120000000LL);

    const std::string s = "a,b\"c\r\nd";
    const std::string csv = binance_capture::csvEscape(s);
    assert(csv == "\"a,b\"\"c\r\nd\"");

    assert(binance_capture::scaledIntegerFromString("83983.14000000", binance_capture::PRICE_SCALE, "price") == 8398314000000LL);
    assert(binance_capture::scaledIntegerFromString("0.00007000", binance_capture::QTY_SCALE, "qty") == 7000LL);

    std::cout << "order_book_tests passed\n";
    return 0;
}
