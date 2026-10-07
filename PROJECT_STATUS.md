# Project Status

## Current phase

Mandatory implementation complete; strict assignment-compliance audit in progress.

## Completed

- C++17/CMake Binance collector for Spot and USD-M combined streams.
- Lowercase combined stream names and uppercase CSV symbol values.
- Combined-envelope parsing and market-data CSV output of the inner payload.
- Fixed-point order-book state and 26-column top-five CSV output.
- Spot `U/u` and USD-M `U/u/pu` sequence validation; depth5 sequence baseline; reconnect resets the book and waits for refresh.
- SIGINT/SIGTERM stop handling, synchronous CSV flushes, and WebSocket/TLS/socket RAII teardown.
- Documentation now describes the actual CLI, schema, integer scales, timestamps, event mapping, reconnects, and limitations.

## Verified

- Windows Release build with MSYS2 MinGW-w64 GCC 16.1.0 and C++17.
- The project and registered test target compile in Release with assertions explicitly enabled for tests. CTest passed after the sequence tests were introduced; the latest expanded test binary could not be launched because Windows Application Control blocked it.
- Earlier live Spot capture connected and produced trade, depth_diff, and depth5 rows; structural CSV inspection found 9 market-data columns and 26 order-book columns per row.
- Offline sequence evaluation of that capture found 36 depth5 baselines, 36 depth diffs already covered by those baselines, and no gaps. Live Spot evidence predates the latest sequence/reconnect changes. Windows Application Control blocked launching the rebuilt collector during this audit, so current-source live behavior is not yet verified.

## Current design

- Dependencies: Boost.Asio/Beast, OpenSSL, nlohmann/json, CMake.
- Output: `market_data.csv` and `order_book.csv`, overwritten per run.
- One symbol and one connection per run; `shard_id` is 0.
- `conn_epoch` starts at 0 and advances after a connection failure; `conn_seq` restarts at 1 for each epoch.
- Price and quantity scale: `100000000` (`10^8`) for both.
- Timestamps: local `system_clock` wall time sampled immediately after `ws.read` completes; order-book rows reuse the corresponding event time.
- Book: depth5 replaces the modeled top-five state; accepted depth diffs mutate price levels; zero quantity deletes; trades do not mutate the book.
- Sequence gaps and reconnects suppress diffs until a depth5 refresh supplies a new baseline.
- I/O: single-threaded blocking reads and synchronous writes with per-row flushes.

## Remaining verification

- Run the rebuilt executable through Windows Application Control and confirm live Spot event/order-book output after the latest sequence changes.
- Verify a live USD-M connection and output.
- Verify signal shutdown/reconnect behavior on the rebuilt executable and capture a 1–2 minute sample.
- Run the latest expanded CTest binary in an environment where execution is permitted.
- A complete REST snapshot/diff-buffer resynchronization is not implemented; this is outside the current mandatory implementation scope as treated by this project.
