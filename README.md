# Binance Market-Data Collector

This project collects Binance public market-data streams for the Spot and USD-M venues and writes the raw inbound event stream plus a top-5 order-book snapshot to CSV files.

## Purpose

The application connects to Binance WebSocket combined streams, validates the JSON payload, writes every processed event to a market-data CSV, maintains a local top-of-book model from the depth streams, and emits order-book snapshots after each applicable book event.

## Architecture

- Binance WebSocket client over TLS/WSS
- Combined-stream JSON parser
- Market-data CSV writer
- Local order-book engine
- Order-book CSV writer
- Signal-aware reconnect logic

## Compiler and standard

- C++17
- CMake
- Recommended build: `cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-12 -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"`
- If the local toolchain differs, adapt the compiler accordingly.

## Dependencies

- Boost.Asio / Boost.Beast
- OpenSSL
- nlohmann/json

## Build

```bash
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-12 -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"
cmake --build build --parallel
```

## Run

```bash
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output
./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output
```

## Binance URLs

- Spot: `wss://stream.binance.com:9443/stream?streams=...`
- USD-M: `wss://fstream.binance.com/public/stream?streams=...`

## Stream list

`<symbol>@depth@100ms` and `<symbol>@depth5@100ms` and `<symbol>@trade`

## CLI

Required flags:

- `--venue spot|usdm`
- `--symbols SYMBOL`
- `--output-dir PATH`

Symbols are normalized to uppercase.

## Market-data CSV

Header:

```text
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

## Order-book CSV

Header:

```text
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

## Timestamp policy

The collector uses local receive time. Each event is stamped when the complete WebSocket message has been received.

## Integer scaling

The implementation uses deterministic fixed-point integer scaling. The order book stores integer prices and sizes and avoids floating-point arithmetic.

## Order-book semantics

- `depth_diff` modifies the local book by price-level updates.
- `depth5` acts as a snapshot refresh for the top-of-book view and is handled separately from normal differential updates.
- `trade` events are written to the market-data CSV but do not mutate the local order book.

## Sequence handling

Depth sequence gaps are detected using Binance `U/u/pu` information. A gap marks the local book out-of-sync, and the application waits for a valid depth5 refresh before resuming differential application.

## Reconnect behavior

The client reconnects after unexpected disconnects using a small backoff and increments the connection epoch on each new connection.

## CSV escaping

The CSV writer uses RFC4180-compatible escaping. Fields containing commas, quotes, CR, or LF are enclosed in quotes and embedded quotes are doubled.

## Current limitations

- Single-symbol, single-connection implementation for the mandatory assignment
- No REST resynchronization yet
- No advanced multi-shard or multi-connection support
- No optional extras beyond the assignment requirements
