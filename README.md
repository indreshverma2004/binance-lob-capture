# Binance Market-Data Collector

A C++ Binance public-market-data collector for Spot and USD-M Futures. It captures combined WebSocket streams and maintains a local top-five order-book view for one configured symbol per run.

## Language, Toolchain, and Dependencies

The project is C++17, configured with CMake. The current Linux/WSL build was validated with GCC 13.3.0 (`/usr/bin/c++`) and CMake 3.28.3. An earlier Windows/MSYS2 build used MinGW-w64 GCC 16.1.0; the latest source changes were validated in Linux/WSL. CMake enables `-Wall -Wextra` (and `/W4` with MSVC).

Dependencies are Boost.Asio/Beast, Boost.System, Boost.Thread, OpenSSL, and nlohmann/json 3.2 or newer. Ninja is used by the commands below.

On Ubuntu/WSL, install dependencies and build/test:

```bash
sudo apt-get update
sudo apt-get install -y cmake ninja-build g++ libssl-dev libboost-all-dev nlohmann-json3-dev
cmake -S . -B build-linux -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-linux --parallel
ctest --test-dir build-linux --output-on-failure
```

## Supported Venues and Streams

- Spot: `wss://stream.binance.com:9443/stream?streams=`
- USD-M Futures: `wss://fstream.binance.com/public/stream?streams=`

For each run, the lowercase-symbol combined path subscribes to:

```text
<symbol>@depth@100ms/<symbol>@depth5@100ms/<symbol>@trade
```

`depth@100ms` is a differential price-level update. `depth5@100ms` is a partial top-of-book snapshot used to seed or refresh the modeled top five, not a differential update. `trade` carries individual trades and does not change the differential order book.

## Data Flow

```text
Binance WebSocket
-> parse combined-stream envelope
-> extract inner data object
-> sample local receive timestamp and assign connection sequence metadata
-> write market_data.csv
-> process a relevant depth diff or depth5 refresh
-> emit the resulting order-book snapshot
```

Market-data events are logged in socket processing order. Trades are captured in the market CSV but do not emit or mutate order-book state.

## Command Line

```text
binance_capture --venue spot|usdm --symbols SYMBOL --output-dir PATH
```

Examples:

```bash
./build-linux/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output-linux
./build-linux/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output-usdm
```

Exactly one uppercase or lowercase symbol is supported per run. The CSV symbol is uppercase; the WebSocket stream name is lowercase. Comma-separated multi-symbol input is rejected. The run creates or truncates `market_data.csv` and `order_book.csv` in the selected output directory.

## Market-Data CSV

The UTF-8 CSV contains one row per valid combined-stream event. `payload_json` is the compact inner Binance `data` object, not the combined-stream wrapper. Its exact header is:

```text
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

Fields:

- `recv_tsec`, `recv_tnsec`: local wall-clock receive time, split into integer seconds and nanosecond remainder.
- `venue`: `spot` or `usdm`.
- `stream_kind`: `depth_diff`, `depth5`, or `trade`.
- `shard_id`: 0 for this single-connection implementation.
- `conn_epoch`: starts at 0 and increments after a failed connection; no prior sequence state is carried into the next epoch.
- `conn_seq`: starts at 1 and increases in event processing order within the epoch.
- `symbol`: uppercase configured symbol.
- `payload_json`: compact JSON representation of the inner event object.

Fields containing comma, quote, CR, or LF are enclosed in double quotes and internal quotes are doubled, following RFC 4180-style CSV escaping.

## Order-Book CSV

One row is emitted for every accepted differential update and every depth5 refresh. The exact 26-column header is:

```text
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

- `tsec`, `tnsec`: the same receive timestamp as the source market event.
- `seqNo`: monotonically increasing application sequence, one increment per emitted row.
- `id`: stable FNV-1a hash of the uppercase symbol, reduced to the positive int32 range.
- `type`: `D` for applied differential depth, `S` for a depth5 snapshot/refresh.
- `side`: `N`, because each row contains both bid and ask sides.
- `bid0`…`bid4` and `ask0`…`ask4`: best five prices first; bids descending, asks ascending.
- `bid_size0`…`bid_size4` and `ask_size0`…`ask_size4`: quantities corresponding to those price levels.

All price and quantity fields are scaled signed int64 values. Unused top-five slots are zero-filled. A differential update with quantity zero removes that price level. A depth5 event replaces the modeled top-five state. Trades never mutate the book.

## Integer Scaling and Timestamps

Binance prices and quantities arrive as decimal strings. Both use a fixed scale of `100000000` ($10^8$). The parser converts decimal digits directly to int64 with checked integer arithmetic; it uses no binary floating-point conversion for price/quantity CSV fields. Int64 overflow and nonzero precision beyond the scale are rejected; extra trailing fractional zeroes are accepted without changing the value.

Timestamps use local `std::chrono::system_clock` wall time sampled immediately after a complete WebSocket message read returns. They are stored as integer seconds plus an integer nanosecond remainder in `[0, 999999999]`. An order-book row reuses its source market event's timestamp. Wall-clock corrections can make timestamps move backward; `conn_seq` records processing order and is not derived from the wall clock.

## Differential Sequence and Reconnect Policy

- Spot depth diffs validate `U <= u`. Events with `u` no newer than the current baseline are stale and ignored. A diff is accepted when its `U` range covers the next expected update; `U > last_update_id + 1` is a gap.
- USD-M depth5 uses its `b`/`a` partial levels and `u` as the snapshot baseline. Spot depth5 uses `bids`/`asks` and `lastUpdateId`.
- USD-M diffs validate `U <= u` and use `U/u/pu`. The first non-stale diff after a depth5 baseline may overlap/bridge that baseline; after that, `pu` must equal the previous final update ID. A mismatch is a gap.
- A gap marks the book out of sync. Diffs are not applied until a depth5 refresh replaces the modeled state and establishes a new baseline.
- On a connection failure, the local book, sequence baseline, and initial-overlap state are reset; `conn_epoch` increments and `conn_seq` restarts at 1 after reconnection. The reconnect delay is two seconds.

## I/O, Shutdown, and Limitations

Socket reads and CSV writes are synchronous on one thread. Each market row and emitted book row is flushed immediately; disk latency can therefore block message processing. SIGINT/SIGTERM set a stop flag. After the blocking read returns, the client attempts a normal WebSocket close and closes both CSV files. RAII owns the WebSocket, TLS stream, socket, and I/O context.

The local book is a top-five model, not a complete exchange book. There is no REST snapshot plus diff-buffer resynchronization, replay mode, multi-symbol support, or sharding. A replay mode is not provided, so deterministic end-to-end regeneration from a saved fixed input has not been verified. Gap recovery and reconnect recovery are implemented/documented but were not dynamically exercised in the final captures.

## Validation

The final Linux/WSL build used GCC 13.3.0 and CMake 3.28.3. The clean Release build completed with `-Wall -Wextra`; CTest passed 1/1 and the test executable passed directly.

Spot capture: `output-linux-90s/` contains the latest paired BTCUSDT run made with a 90-second limit. Its event timestamps span 84.245585 seconds; three receive timestamps moved backward by approximately 1.4–1.6 seconds, while `conn_seq` remained ordered. It recorded 3,980 market rows: 2,288 trades, 846 depth5 events, and 846 depth diffs; 846 order-book rows were replay-checked. All three stream types arrived, CSV/JSON schemas validated, and every order-book row matched its depth5 source snapshot. All 846 diffs were stale against the latest snapshot baseline, so this capture did not demonstrate applied Spot diffs or deletions. No gaps or reconnects occurred.

USD-M capture: `output-usdm/` contains a paired approximately 24-second BTCUSDT run. It recorded 980 market rows: 238 depth diffs, 231 depth5 events, and 511 trades; 466 order-book rows replay-matched. The replay found 235 applied diffs, 3 stale diffs, and no gaps. It observed 1,750 zero-quantity updates, of which 5 removed existing modeled levels. All three streams arrived; schemas, timestamps, ordering, fixed-point values, and replay consistency validated. No reconnect occurred.

## GitHub Submission

Do not push until the destination remote has been checked. This workspace currently has an existing `origin` pointing to a repository unrelated to this assignment; no remote has been changed and no push has been made. Create a dedicated submission repository and use a separately named remote until the destination is confirmed:

```bash
git remote -v
git remote add submission https://github.com/<your-username>/binance-lob-capture.git
git push -u submission main
git tag -a v1.0.0 -m "Submission v1.0.0"
git push submission v1.0.0
```

The project `.gitignore` excludes build outputs, ordinary run directories, compiled artifacts, and local secret configuration. The `output-linux-90s/` Spot sample is intentionally not ignored so it can be included as the required sample. Never submit credentials, API keys, or private keys.
