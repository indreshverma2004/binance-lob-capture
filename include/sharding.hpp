#pragma once

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace binance_capture {

constexpr size_t DEFAULT_SYMBOLS_PER_SHARD = 10;

inline std::vector<std::vector<std::string>> shardSymbols(
    const std::vector<std::string>& symbols,
    size_t symbols_per_shard = DEFAULT_SYMBOLS_PER_SHARD) {
    if (symbols_per_shard == 0) {
        throw std::invalid_argument("symbols_per_shard must be positive");
    }
    std::vector<std::vector<std::string>> shards;
    for (size_t offset = 0; offset < symbols.size(); offset += symbols_per_shard) {
        const size_t end = std::min(symbols.size(), offset + symbols_per_shard);
        shards.emplace_back(symbols.begin() + static_cast<std::ptrdiff_t>(offset),
                            symbols.begin() + static_cast<std::ptrdiff_t>(end));
    }
    return shards;
}

}  // namespace binance_capture
