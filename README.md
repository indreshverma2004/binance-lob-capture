# Binance Market-Data Collector

A C++17 command-line collector for Binance Spot and USD-M public combined streams. One run writes an inbound-event audit CSV and an order-book CSV for one symbol.

## Compiler, Standard, and Dependencies

The audited Windows build used MSYS2 MinGW-w64 GCC 16.1.0, C++17, CMake, Boost.Asio/Beast, OpenSSL, and nlohmann/json. CMake also requires Boost.System and Boost.Thread packages.

For Ubuntu/Debian, install dependencies and build with:

```bash
sudo apt-get update
sudo apt-get install -y cmake ninja-build g++-12 libssl-dev libboost-all-dev nlohmann-json3-dev
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-12 -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

On Windows/MSYS2, install the MinGW-w64 GCC, Boost, OpenSSL, nlohmann-json, and Ninja packages, then configure CMake with the selected MinGW compiler and package prefix. TLS verification uses the platform default trust paths and, when present, the MSYS2 CA bundle at `C:/msys64/usr/ssl/certs/ca-bundle.crt`.

## Run

The exact CLI is:

```text
binance_capture --venue spot|usdm --symbols SYMBOL --output-dir PATH
```

Examples:

```bash
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output
./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output
```

One uppercase or lowercase symbol is accepted per run; the market CSV stores it uppercase. A comma-separated list is rejected rather than partially captured. The run creates or truncates `market_data.csv` and `order_book.csv` in the output directory, so preserve prior runs separately.

## WebSocket Streams

Spot connects to `wss://stream.binance.com:9443/stream?streams=`. USD-M connects to `wss://fstream.binance.com/public/stream?streams=`. Stream names use a lowercase symbol and subscribe to all three required suffixes:

```text
<symbol>@depth@100ms/<symbol>@depth5@100ms/<symbol>@trade
```

The combined-message envelope is parsed and only its inner `data` object is written as `payload_json`. `stream_kind` is `depth_diff`, `depth5`, or `trade`.

## Market-Data CSV

The file is UTF-8, one row per valid combined-stream event processed, in socket processing order. The exact header is:

```text
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

Receive time is local `std::chrono::system_clock` wall time sampled immediately after the complete WebSocket message read returns; seconds and the nanosecond remainder are written as integers. `conn_seq` starts at 1 and increases within each `conn_epoch`; `shard_id` is 0 for the single connection. JSON is compact/minified. CSV fields containing comma, quote, CR, or LF are quoted and embedded quotes are doubled (RFC 4180 escaping).

## Order-Book CSV and Semantics

The exact 26-column header is:

```text
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

One row is emitted for each accepted differential update and each depth5 refresh; trades do not mutate or emit an order-book row. The row timestamp is the same receive timestamp as its market-data event. `seqNo` increments once per emitted row. `id` is a stable FNV-1a hash of the uppercase symbol, reduced to the positive signed-int32 range. `type` is `D` for an applied depth diff and `S` for a depth5 snapshot. `side` is `N` because each row contains both sides. Bids are sorted highest-first and asks lowest-first. Missing price/size slots are integer zeroes.

Depth diffs apply price-level quantities; quantity zero deletes that level. A depth5 event replaces the modeled book and supplies its sequence baseline. Spot depth5 uses `bids`/`asks` and `lastUpdateId`; USD-M partial-depth events use `b`/`a` and the final update ID `u` (with `U/u/pu` sequence fields). Prices and quantities are parsed from Binance decimal strings with no binary floating-point conversion: both use scale `100000000` ($10^8$). Nonzero precision beyond that scale and int64 overflow are rejected.

Spot diffs use `U/u`: stale events are ignored, overlapping events are applied if their range covers the next expected update ID, and a skipped range marks the book out-of-sync. USD-M diffs validate `U <= u`; after a depth5 baseline, the first non-stale diff may overlap/bridge that baseline, then subsequent diffs require `pu` to equal the previous final update ID. A skipped range marks the book out-of-sync. While out-of-sync, diffs are not applied; a depth5 refresh restores the modeled top-five book and sequence baseline. On connection failure the book and baseline are discarded, `conn_epoch` increments, `conn_seq` resets, and diffs are ignored until a new depth5 refresh. Reconnect delay is two seconds.

## I/O, Shutdown, and Limitations

Socket reads and CSV writes run synchronously on one thread; each market row and order-book row is flushed immediately. This avoids a writer queue but lets disk latency block stream processing. SIGINT and SIGTERM set a stop flag; after the current blocking read returns, the client attempts a normal WebSocket close and closes the CSV files. A socket failure tears down the WebSocket/TLS/socket objects by RAII before reconnecting.

This version supports one symbol and one connection per run. It does not fetch a REST depth snapshot: the local model is a top-five view seeded/refreshed from the depth5 stream, not a complete exchange book. USD-M live operation and reconnect recovery still require a live run in an environment that permits the rebuilt executable.

The most recent available Spot sample is in `output/market_data.csv` and `output/order_book.csv`; it spans about 3.6 seconds, predates the latest sequence changes, and is not the assignment's suggested 1–2 minute sample.
