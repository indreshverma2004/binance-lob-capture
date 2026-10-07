# Project Status

## Current phase

Phase 1: project foundation and fail-first tests.

## Completed

- Empty workspace confirmed.
- Initial CMake project scaffold created.
- Initial README.md created.
- Initial PROJECT_STATUS.md created.
- Initial order-book regression tests created to drive the implementation.

## In progress

- Implementing the core C++ order-book logic and Binance WebSocket collector.

## Not implemented yet

- Binance TLS/WSS client
- combined JSON parsing
- market-data CSV writer
- order-book CSV writer
- real reconnect and graceful shutdown logic
- live capture verification

## Important design decisions

- C++ standard: C++17
- compiler: system default or g++-12 when available
- dependencies: Boost.Asio/Beast, OpenSSL, nlohmann/json
- timestamp policy: local wall-clock receive time
- price scale: deterministic fixed-point integer scale
- quantity scale: deterministic fixed-point integer scale
- shard_id policy: 0 for the mandatory single-connection implementation
- conn_epoch policy: starts at 0 and increments after reconnect
- conn_seq policy: monotonic within each connection epoch
- order-book semantics: depth updates modify bids and asks; trade events do not mutate the book
- depth5 semantics: snapshot/refresh semantics handled separately from normal diff updates
- trade semantics: written to market-data CSV only
- sequence/gap policy: detect Binance depth gaps and stop applying diffs until a depth5 refresh
- reconnect policy: reconnect after unexpected disconnect with backoff delay
- CSV naming: market_data.csv and order_book.csv in the chosen output directory

## Last known good build

Not yet available.

## Last known good run

Not yet available.

## Tests

- Initial order-book regression tests added; implementation pending.

## Known limitations

- This is the mandatory single-symbol, single-connection version only.
- No REST snapshot logic yet.
- No optional features implemented.
