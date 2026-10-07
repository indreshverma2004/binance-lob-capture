# Project Status

## Current phase

Mandatory implementation complete; final documentation/audit complete.

## Implemented

- C++17/CMake collector for Binance Spot and USD-M Futures public combined streams.
- One configured symbol per run; lowercase stream names and uppercase CSV symbol.
- Combined envelope parsing and inner-payload market-data CSV with receive time and connection metadata.
- Local top-five book: Spot/ USD-M depth5 refreshes, differential updates, zero-quantity removal, and sorted best-five output.
- Fixed-point decimal-string scaling at `10^8` with int64 overflow and excessive-precision checks; no floating-point conversion in price/quantity output.
- Spot `U/u` and USD-M `U/u/pu` stale/gap processing, including the first USD-M diff overlap after a depth5 baseline.
- Reconnect state reset, connection epoch/sequence handling, per-row CSV flushing, SIGINT/SIGTERM stop handling, and RAII socket/TLS teardown.
- README schemas, CLI, sequence policy, toolchain/build instructions, validation results, limitations, and GitHub submission guidance.

## Mandatory Checklist

- PASS — C++ source, CMake build, and required dependencies documented.
- PASS — Live Spot and USD-M combined-stream capture received `depth@100ms`, `depth5@100ms`, and `trade`.
- PASS — Market-data CSV exact header, nine-field records, compact inner-object JSON, RFC4180-style escaping, uppercase symbols, and processing order validated in captured files.
- PASS — Order-book CSV exact 26-column schema and rows, timestamp correspondence, sorted top-five levels, and snapshot replay consistency validated.
- PASS — Differential updates, zero-quantity deletion, depth5 replacement, and trade non-mutation covered by implementation/tests; USD-M diff application and removals were also observed live.
- PASS — `10^8` fixed-point parsing, int64 overflow behavior, and integer timestamps are implemented and tested/documented.
- PASS — Spot and USD-M sequence rules and reconnect reset policy are implemented and documented; stale-event behavior was observed live.
- PASS — Paired approximately 90-second Spot sample exists with the required stream types and CSVs.
- PASS — Required CLI is documented; unsupported multi-symbol input is rejected rather than partially processed.
- NOT VERIFIED — Forced sequence-gap recovery has not been dynamically exercised in either final capture.
- NOT VERIFIED — Forced reconnect recovery, epoch advancement, and new-epoch sequence reset have not been dynamically exercised.
- NOT VERIFIED — End-to-end output regeneration from a fixed saved input; no replay mode is implemented.

## Verified

- Assignment PDF, current source tree, CMake, tests, README, project status, git state, and generated Spot/USD-M CSVs reviewed.
- Linux/WSL Release build using GCC 13.3.0 (`/usr/bin/c++`) and CMake 3.28.3; clean build completed with `-Wall -Wextra` and no compiler warnings reported.
- `ctest --test-dir build-linux --output-on-failure`: 1/1 passed.
- `./build-linux/order_book_tests`: passed directly.
- Spot and USD-M captured CSVs parsed with strict CSV/JSON readers and independently replayed for order-book consistency. Headers/widths, compact inner payloads, CSV quoting, timestamps, ordering, and state equality were checked.

## Spot Validation

- Sample: `output-linux-90s/market_data.csv` and `output-linux-90s/order_book.csv`, regenerated with a 90-second run limit.
- Market event timestamps span 84.245585 seconds; three wall-clock timestamps move backward by 1.4–1.6 seconds. Nanosecond fields remain valid and `conn_seq` preserves processing order.
- Market rows: 3,980 total; 846 `depth_diff`, 846 `depth5`, 2,288 `trade`.
- Order-book rows: 846; all output rows replay-matched. The captured sequence replay classified all 846 diffs as stale relative to the newest depth5 baseline, with no gaps. Thus the Spot sample validates capture/snapshots but does not demonstrate applied Spot differential updates or deletions.
- CSV headers/widths, payload JSON, CSV escaping, uppercase symbol, timestamp field validity, sequence ordering, top-five ordering, and trade non-mutation validated. This capture showed wall-clock reversals, so its timestamps are not monotonic even though row processing order is preserved.

## USD-M Validation

- Sample: `output-usdm/market_data.csv` and `output-usdm/order_book.csv`; approximately 24 seconds.
- Market rows: 980 total; 238 `depth_diff`, 231 `depth5`, 511 `trade`.
- Order-book rows: 466; replay matched all 466 rows.
- Sequence replay: 235 diffs applied, 3 stale, 0 gaps. One connection epoch (0); no reconnect.
- Zero-quantity updates: 1,750 observed; 5 removed levels present in the modeled book.
- CSV schemas, inner payload JSON, escaping, uppercase symbol, timestamps, conn sequence, fixed-point values, top-five ordering, and trade non-mutation validated.

## Important Implementation Decisions

- Price and quantity scales are both `100000000` (`10^8`). Decimal strings are parsed with checked integer arithmetic only.
- Local wall-clock timestamp is sampled immediately after each complete WebSocket read; book rows reuse their source market event time.
- Spot depth5 uses `bids`/`asks` and `lastUpdateId`; Spot diffs use `U/u`, ignore stale events, and gap when the update range skips the next expected ID.
- USD-M depth5 uses partial `b`/`a` and seeds from `u`; USD-M diffs use `U/u/pu`. The first non-stale diff after a snapshot can overlap/bridge it; later diffs require `pu` continuity.
- Depth5 replaces the modeled top-five state. Accepted diffs update levels; quantity zero erases a level. Trades only enter the market CSV.
- A detected gap or connection failure suppresses diffs until a depth5 refresh. A connection failure clears the book and baseline, increments `conn_epoch`, and resets `conn_seq` to 1 for the next epoch.
- One blocking socket-reader/writer thread; market and book rows are flushed synchronously. No REST resync, replay, multi-symbol support, sharding, or metrics are implemented.

## Build

Known-good Linux/WSL commands:

```bash
cmake -S . -B build-linux -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-linux --parallel
ctest --test-dir build-linux --output-on-failure
```

Toolchain: GCC 13.3.0, CMake 3.28.3, C++17. Dependencies: Boost.Asio/Beast, Boost.System, Boost.Thread, OpenSSL, and nlohmann/json.

## Tests

- CTest: 1/1 passed.
- Direct unit test executable: passed.
- Tests cover order-book update/removal/snapshot/top-five behavior, CSV/header contracts, fixed-point boundaries/overflow/precision, stream URL/classification, Spot sequence stale/gap behavior, and USD-M initial overlap/continuity/stale/gap behavior.

## Files Changed Recently

- Implementation and tests: `include/binance_capture.hpp`, `src/binance_capture.cpp`, `src/main.cpp`, `tests/test_order_book.cpp`.
- Build and documentation: `CMakeLists.txt`, `README.md`, `PROJECT_STATUS.md`, `.gitignore`.
- `output-linux-90s/` and `output-usdm/` contain the validation captures; build products are generated artifacts.

## Current Git Status

No commit was made. At handoff, intended changes are `README.md`, `PROJECT_STATUS.md`, and the new `.gitignore`; `output-linux-90s/market_data.csv` and `output-linux-90s/order_book.csv` are untracked sample deliverables intentionally not ignored. Linux build/test execution also modifies tracked `build-linux/.ninja_deps`, `build-linux/.ninja_log`, `build-linux/Testing/Temporary/CTestCostData.txt`, and `build-linux/Testing/Temporary/LastTest.log`. The source/header/test/CMake files are unchanged in this final documentation pass. The local Git `origin` points to `https://github.com/indreshverma2004/ordertracker.git`, which appears unrelated; do not push this assignment to it. Recheck `git status --short` before submission. Local scratch probes `ssl_test.cpp`, `ssl_test.exe`, `test.cpp`, and `test.exe` are not CMake targets and are not part of the tracked project source.

## Next Phase

Mandatory requirements are complete. Next work should focus on optional improvements/stretch features. Do not modify mandatory behavior without a clear reason.
