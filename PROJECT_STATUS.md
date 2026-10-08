# Project Status

## Current phase

Mandatory implementation stable; optional replay, REST snapshot resynchronization, lightweight metrics/replay benchmarking, and multi-symbol sharding are implemented and covered by tests.

## Implemented

- C++17/CMake collector for Binance Spot and USD-M Futures public combined streams.
- Multi-symbol capture with stable input-order groups of up to 10 symbols per WebSocket shard; lowercase stream names and uppercase CSV symbols.
- Combined envelope parsing and inner-payload market-data CSV with receive time and connection metadata.
- Local top-five book: Spot/ USD-M depth5 refreshes, differential updates, zero-quantity removal, and sorted best-five output.
- Fixed-point decimal-string scaling at `10^8` with int64 overflow and excessive-precision checks; no floating-point conversion in price/quantity output.
- Spot `U/u` and USD-M `U/u/pu` stale/gap processing, including the first USD-M diff overlap after a depth5 baseline.
- REST snapshot plus buffered differential recovery for gaps and reconnects, using the shared order-book processor.
- Lightweight live/replay event metrics and replay benchmark reporting, without changing CSV schemas or order-book decisions.
- Per-shard reconnect state reset and `conn_epoch`/`conn_seq`; independent per-symbol book and REST state; serialized shared CSV writes and globally increasing output `seqNo`; per-row flushing, SIGINT/SIGTERM handling, and RAII socket/TLS teardown.
- README schemas, CLI, sequence policy, toolchain/build instructions, validation results, limitations, and GitHub submission guidance.

## Mandatory Checklist

- PASS — C++ source, CMake build, and required dependencies documented.
- PASS — Live Spot and USD-M combined-stream capture received `depth@100ms`, `depth5@100ms`, and `trade`.
- PASS — Market-data CSV exact header, nine-field records, compact inner-object JSON, RFC4180-style escaping, uppercase symbols, and processing order validated in captured files.
- PASS — Order-book CSV exact 26-column schema and rows, timestamp correspondence, sorted top-five levels, and snapshot replay consistency validated.
- PASS — Differential updates, zero-quantity deletion, depth5 behavior, and trade non-mutation are covered by implementation/tests; USD-M diff application and removals were observed live. In REST-backed live mode, REST is authoritative and depth5 is read-only sanity data.
- PASS — `10^8` fixed-point parsing, int64 overflow behavior, and integer timestamps are implemented and tested/documented.
- PASS — Spot and USD-M sequence rules and reconnect reset policy are implemented and documented; stale-event behavior was observed live.
- PASS — Paired approximately 90-second Spot sample exists with the required stream types and CSVs.
- PASS — Required CLI is documented; comma-separated symbols are partitioned into stable WebSocket shards and each symbol is processed independently.
- NOT VERIFIED — Forced sequence-gap recovery has not been dynamically exercised in either final capture.
- NOT VERIFIED — Forced reconnect recovery, epoch advancement, and new-epoch sequence reset have not been dynamically exercised.

## Verified

- Assignment PDF, current source tree, CMake, tests, README, project status, git state, and generated Spot/USD-M CSVs reviewed.
- Linux/WSL Release build using GCC 13.3.0 (`/usr/bin/c++`) and CMake 3.28.3; clean build completed with `-Wall -Wextra` and no compiler warnings reported.
- Metrics feature verification (2026-10-07, WSL): `cmake --build build-linux --parallel` completed successfully; CTest passed 4/4 (`order_book_tests`, `market_replay_tests`, `resync_tests`, `metrics_tests`).
- Replay benchmark as reported by the user: 3,980 events, 846 order-book rows, 0.26 seconds, 15,459.61 events/sec, 3,286.14 rows/sec. Timing is machine/build/filesystem dependent.
- A final clean Release rebuild completed after adding the asynchronous REST client; no compiler warnings were reported.
- The current Spot sample and earlier USD-M capture were parsed with strict CSV/JSON readers and independently replayed for order-book consistency. Multi-symbol/shard replay behavior is covered with deterministic interleaved fixtures. The USD-M CSV files are no longer present in the workspace. Headers/widths, compact inner payloads, CSV quoting, timestamps, ordering, and state equality were checked while available.
- The Spot 90-second market CSV replayed successfully; output matched the original order-book CSV and a second replay byte-for-byte.
- REST snapshot transport and response validation passed mocked tests. Final short live Spot and USD-M runs reached their REST endpoints and established snapshot baselines.

## Date-Stamped Capture Validation (2026-10-08)

- Spot BTCUSDT: 120.33 seconds, 4,995 market events, 1,612 order-book rows, 1,207 applied diffs, 3 stale diffs, and 1 successful REST resynchronization. Files: `output/market_data_spot_BTCUSDT_2026-10-08.csv` and `output/market_data_spot_BTCUSDT_2026-10-08_orderbook.csv`.
- Spot BTCUSDT + ETHUSDT: 120.53 seconds, 12,479 events, 3,883 rows, 2,414 applied diffs, 6 stale diffs, and 2 successful REST resynchronizations. Files: `output/market_data_spot_BTCUSDT-ETHUSDT_2026-10-08.csv` and its `_orderbook.csv` companion. Both symbols were captured on shard 0 with independent books.
- USD-M BTCUSDT: 120.45 seconds, 4,865 events, 1,509 rows, 1,194 applied diffs, 4 stale diffs, and 1 successful REST resynchronization. Files: `output/market_data_usdm_BTCUSDT_2026-10-08.csv` and `output/market_data_usdm_BTCUSDT_2026-10-08_orderbook.csv`.
- All six files passed exact-header and column-width checks; market sequence metadata was contiguous per shard/epoch and order-book `seqNo` increased from 1 without gaps. No reconnects were observed.

## Spot Validation

- Sample: `output-linux-90s/market_data.csv` and `output-linux-90s/order_book.csv`, regenerated with a 90-second run limit.
- Market event timestamps span 84.245585 seconds; three wall-clock timestamps move backward by 1.4–1.6 seconds. Nanosecond fields remain valid and `conn_seq` preserves processing order.
- Market rows: 3,980 total; 846 `depth_diff`, 846 `depth5`, 2,288 `trade`.
- Order-book rows: 846; all output rows replay-matched. The captured sequence replay classified all 846 diffs as stale relative to the newest depth5 baseline, with no gaps. Thus the Spot sample validates capture/snapshots but does not demonstrate applied Spot differential updates or deletions.
- CSV headers/widths, payload JSON, CSV escaping, uppercase symbol, timestamp field validity, sequence ordering, top-five ordering, and trade non-mutation validated. This capture showed wall-clock reversals, so its timestamps are not monotonic even though row processing order is preserved.

## USD-M Validation

- Historical live capture: approximately 24 seconds; its `output-usdm/` CSV files are not present in the current workspace.
- Market rows: 980 total; 238 `depth_diff`, 231 `depth5`, 511 `trade`.
- Order-book rows: 466; replay matched all 466 rows.
- Sequence replay: 235 diffs applied, 3 stale, 0 gaps. One connection epoch (0); no reconnect.
- Zero-quantity updates: 1,750 observed; 5 removed levels present in the modeled book.
- CSV schemas, inner payload JSON, escaping, uppercase symbol, timestamps, conn sequence, fixed-point values, top-five ordering, and trade non-mutation validated.

## Important Implementation Decisions

- Price and quantity scales are both `100000000` (`10^8`). Decimal strings are parsed with checked integer arithmetic only.
- Local wall-clock timestamp is sampled immediately after each complete WebSocket read; book rows reuse their source market event time. A REST response has no market-event receive timestamp, so it creates no order-book row; buffered events retain their original timestamps.
- Spot depth5 uses `bids`/`asks` and `lastUpdateId`; Spot diffs use `U/u`, ignore stale events, and gap when the update range skips the next expected ID.
- USD-M depth5 uses partial `b`/`a` and seeds from `u`; USD-M diffs use `U/u/pu`. The first non-stale diff after a snapshot can overlap/bridge it; later diffs require `pu` continuity.
- Live resync uses `GET /api/v3/depth` on `api.binance.com` for Spot and `GET /fapi/v1/depth` on `fapi.binance.com` for USD-M, with uppercase symbol and limit 1000. REST snapshots are authoritative and replace the book; no CSV row is emitted for the response because it has no market-event receive timestamp.
- Up to 2,048 depth diffs are buffered while resynchronizing. Stale updates are discarded; only a fully validated bridge/continuity chain commits. Buffer overflow clears the buffer and requests a fresh snapshot. Async HTTPS has a 10-second deadline; retry delay is 2, 4, 8, 16, then 30 seconds, with longer `Retry-After` honored for 418/429.
- depth5 remains a read-only partial sanity observation in REST mode. Mismatches are logged at most once per connection and do not replace the REST book or initiate repeated REST requests.
- In replay mode, depth5 seeds/replaces the replayed modeled top-five state. In live REST mode, REST is authoritative and depth5 is read-only sanity data. Accepted diffs update levels; quantity zero erases a level. Trades only enter the market CSV.
- A detected gap or connection failure suppresses diffs until a REST snapshot plus buffered bridge succeeds. A connection failure clears the book and baseline, increments `conn_epoch`, and resets `conn_seq` to 1 for the next epoch.
- One blocking WebSocket reader/writer thread per shard; each symbol within a shard owns a separate processor and bounded asynchronous REST snapshot request. Shared market/book CSV writes are mutex-serialized and synchronously flushed. Replay validates interleaved per-shard sequences and resets only books belonging to a shard whose epoch changes.

## Build

Known-good Linux/WSL commands:

```bash
cmake -S . -B build-linux -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-linux --parallel
ctest --test-dir build-linux --output-on-failure
```

Toolchain: GCC 13.3.0, CMake 3.28.3, C++17. Dependencies: Boost.Asio/Beast, Boost.System, Boost.Thread, OpenSSL, and nlohmann/json.

## Tests

- Current WSL CTest run after multi-symbol sharding changes: 4/4 passed (`order_book_tests`, `market_replay_tests`, `resync_tests`, and `metrics_tests`).
- Prior direct runs of `order_book_tests`, `market_replay_tests`, and `resync_tests` passed.
- Tests cover order-book updates/deletions, CSV/header contracts, fixed-point limits, Spot/USD-M sequence rules, mocked REST parsing/failures/rate limits, buffered recovery, reconnect-state isolation, bounded overflow, depth5 sanity, replay determinism, metrics counters/rates, and benchmark replay output equivalence.

## Metrics and Benchmark

- Live shutdown reports monotonic runtime, event counts by stream, emitted rows, applied/stale diffs, zero-quantity updates observed in depth-diff messages, reconnects, successful REST resynchronizations, REST snapshot failures, and event throughput.
- `--benchmark` is available with `--replay`; it runs the existing replay processor, writes the standard `order_book.csv`, and reports elapsed time, events/sec, and rows/sec.
- Benchmark command: `./build-linux/binance_capture --replay ./output/market_data_spot_BTCUSDT_2026-10-08.csv --output-dir ./benchmark-output --benchmark`.
- Invalid events abort replay/capture and are not included in the normal completion summary; no rejected-event counter is reported.
- Latest reported run: 3,980 events and 846 rows in 0.26 seconds (15,459.61 events/sec; 3,286.14 rows/sec). These values are environment-specific.

## Files Changed Recently

- Implementation and tests: `include/binance_capture.hpp`, `include/market_replay.hpp`, `include/rest_snapshot_client.hpp`, `include/sharding.hpp`, `src/binance_capture.cpp`, `src/market_replay.cpp`, `src/rest_snapshot_client.cpp`, `src/main.cpp`, `tests/test_order_book.cpp`, `tests/test_replay.cpp`, `tests/test_resync.cpp`.
- Build and documentation: `CMakeLists.txt`, `README.md`, `PROJECT_STATUS.md`, `.gitignore`.
- `output-linux-90s/` contains the current Spot validation capture. USD-M capture counts are historical; its CSV artifacts are not present in the workspace. Build products are generated artifacts.

## Current Git Status

No commit was made. The current worktree includes multi-symbol, duration, and date-stamped output changes in `README.md`, `PROJECT_STATUS.md`, `include/sharding.hpp`, `src/main.cpp`, `src/market_replay.cpp`, and `tests/test_replay.cpp`. Three date-stamped live capture pairs were created in `output/`; the USD-M pair was also copied into `output-usdm/` for GitHub inclusion. The prior generic output files were preserved. The Linux build updated tracked generated files under `build-linux/`. Other pre-existing worktree changes remain; review `git status --short` before staging. The branch is `main`, tracking `origin/main`; `origin` points to `https://github.com/indreshverma2004/ordertracker.git`, which appears unrelated to this assignment. Do not push this assignment to that remote.

## Optional Features Completed

### Multi-Symbol Capture and Sharding

- Status: implemented; capture supports comma-separated unique symbols for Spot or USD-M.
- Assignment: symbols retain input order and are partitioned into stable zero-based groups of at most 10; one WebSocket connection/thread serves each group.
- State scope: `conn_epoch` and contiguous `conn_seq` belong to a shard connection; every symbol has an independent order-book processor, depth sequence, and REST snapshot buffer/recovery. Reconnect resets all symbol books on that shard and advances only that shard's epoch.
- Output: workers share the two CSV files through a mutex; market rows from different shards may interleave. Order-book rows have one global monotonically increasing `seqNo`; `id` identifies the symbol.
- Replay: interleaved symbols/shards are supported; sequence continuity is checked separately per shard and epoch changes reset only that shard's books. Replay remains offline.
- Validation: the replay test suite covers fixed shard grouping, interleaved per-shard records, independent symbol book state, global output sequence, and shard epoch reset. Linux build passed and CTest passed 4/4. Live multi-symbol validation has not yet been run.

### Replay Mode

- Status: implemented and validated as an optional feature; mandatory status above remains unchanged.
- CLI: `binance_capture --replay MARKET_CSV --output-dir PATH`.
- Tests: `market_replay_tests` covers valid replay, RFC4180 escaping, depth diff/snapshot/trade behavior, timestamps, determinism, epoch sequence preservation, malformed CSV/JSON, bad columns, invalid timestamps/numerics, unsupported stream kinds, and missing sequence fields.
- Validation input: `output-linux-90s/market_data.csv`; 3,980 events (846 depth_diff, 846 depth5, 2,288 trades), 846 output rows, 0 applied diffs, 846 stale diffs, 0 gaps, and 0 rejected rows.
- Replay matched the original Spot `order_book.csv` byte-for-byte. Two independent runs produced byte-identical output with SHA-256 `b3732c1ecd0b01928e8a82b2c32756beded838d0e0185f233525b50037e1d4bb`.
- Changed files: `CMakeLists.txt`, `README.md`, `PROJECT_STATUS.md`, `.gitignore`, `include/market_replay.hpp`, `src/market_replay.cpp`, `src/main.cpp`, and `tests/test_replay.cpp`.

### REST Snapshot + Buffered Resynchronization

- Status: implemented as an optional recovery feature; mandatory live/replay schemas and fixed-point behavior remain unchanged.
- Endpoints: Spot `https://api.binance.com/api/v3/depth`; USD-M `https://fapi.binance.com/fapi/v1/depth`; both request uppercase symbol and limit 1000.
- Rules: Spot stale `u <= L` is discarded and first bridge must cover `L+1`; USD-M uses `U/u/pu`, permits the first diff to bridge REST `lastUpdateId`, then requires `pu` continuity. Subsequent gaps trigger another resync.
- Buffer: at most 2,048 differential events. Overflow clears the buffer and invalidates any in-flight snapshot response before a delayed retry.
- Recovery: REST snapshot is staged as authoritative full state; buffered events are replayed through the same checked sequence and order-book processor. Rows are emitted only for buffered/applied WebSocket events, retaining their receive timestamps; no timestamp is fabricated for REST.
- Retry: asynchronous verified HTTPS request with 10-second deadline; 2/4/8/16/30-second backoff, honoring longer HTTP 418/429 `Retry-After`.
- Tests: `resync_tests` covers valid Spot/USD-M snapshots, missing/malformed fields and levels, numeric validation, Spot/U-u and USD-M/U-u-pu bridges, stale discard, ordered multi-event replay, gaps, reconnect-state isolation, zero deletion, buffer overflow, depth5 sanity, mocked transport failures, HTTP 418/429 Retry-After, and retry-to-success.
- Snapshot parsing: PASS in deterministic Spot and USD-M fixtures and in short live REST requests.
- Buffering: PASS in deterministic tests for event order, stale discard, the 2,048-event cap, overflow clearing, and subsequent snapshot recovery.
- Spot and USD-M sequence recovery: PASS in deterministic fault injection for valid bridge, stale updates, a second sequence gap, and USD-M `pu` mismatch.
- Gap detection: PASS in deterministic tests. NOT VERIFIED during live capture; no natural gap occurred.
- Reconnect recovery: PASS for state reset in deterministic tests. NOT VERIFIED as a live network reconnect; no reconnect occurred.
- REST failure handling: PASS in mocked malformed JSON/levels, missing fields, simulated connection/TLS failures, HTTP 418/429 `Retry-After`, and retry-to-success. A live REST failure/timeout was not injected.
- Fault injection: PASS; simulated gap/reconnect recovery uses mocked snapshots and requires no Internet.
- Live: PASS for initial REST snapshot/bridge on both venues. Final 12-second Spot run fetched snapshot `101129856626`, buffered 7, discarded 5 stale and applied 2. Final 12-second USD-M run fetched snapshot `11753732261170`, buffered 5 and discarded all 5 stale. Neither had a natural gap/reconnect; live gap/reconnect recovery remains NOT VERIFIED. A depth5 sanity mismatch did not initiate REST retries.
- Files changed for this optional feature: `CMakeLists.txt`, `include/market_replay.hpp`, `include/rest_snapshot_client.hpp`, `src/market_replay.cpp`, `src/rest_snapshot_client.cpp`, `src/main.cpp`, `tests/test_resync.cpp`, `README.md`, and `PROJECT_STATUS.md`.

## Next Optional Features

- CSV validation tooling.
- Additional fault-injection and live reconnect testing.

Mandatory requirements are complete. Do not modify mandatory behavior without a clear reason.
