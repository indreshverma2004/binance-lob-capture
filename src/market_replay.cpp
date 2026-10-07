#include "market_replay.hpp"
#include "metrics.hpp"

#include <algorithm>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace binance_capture {

namespace {

std::vector<std::string> parseCsvRecord(std::istream& input,
                                        size_t& physical_line,
                                        size_t record_number) {
    std::vector<std::string> fields;
    std::string field;
    bool in_quotes = false;
    bool quote_closed = false;
    bool at_field_start = true;
    bool saw_record_data = false;

    auto fail = [record_number](const std::string& reason) -> void {
        throw std::runtime_error("market_data.csv row " + std::to_string(record_number) + ": " + reason);
    };

    while (true) {
        const int next = input.get();
        if (next == std::char_traits<char>::eof()) {
            if (input.bad()) {
                fail("I/O error while reading CSV");
            }
            if (!saw_record_data && fields.empty() && field.empty()) {
                return {};
            }
            if (in_quotes) {
                fail("unterminated quoted CSV field");
            }
            fields.push_back(std::move(field));
            return fields;
        }

        const char ch = static_cast<char>(next);
        saw_record_data = true;

        if (in_quotes) {
            if (ch == '"') {
                if (input.peek() == '"') {
                    input.get();
                    field.push_back('"');
                } else {
                    in_quotes = false;
                    quote_closed = true;
                }
            } else {
                field.push_back(ch);
                if (ch == '\n') {
                    ++physical_line;
                }
            }
            continue;
        }

        if (quote_closed) {
            if (ch == ',') {
                fields.push_back(std::move(field));
                field.clear();
                quote_closed = false;
                at_field_start = true;
                continue;
            }
            if (ch == '\n') {
                ++physical_line;
                fields.push_back(std::move(field));
                return fields;
            }
            if (ch == '\r' && input.peek() == '\n') {
                input.get();
                ++physical_line;
                fields.push_back(std::move(field));
                return fields;
            }
            fail("unexpected character after closing CSV quote");
        }

        if (ch == ',') {
            fields.push_back(std::move(field));
            field.clear();
            at_field_start = true;
        } else if (ch == '\n') {
            ++physical_line;
            fields.push_back(std::move(field));
            return fields;
        } else if (ch == '\r') {
            if (input.peek() != '\n') {
                fail("bare carriage return outside a quoted CSV field");
            }
            input.get();
            ++physical_line;
            fields.push_back(std::move(field));
            return fields;
        } else if (ch == '"') {
            if (!at_field_start) {
                fail("quote inside an unquoted CSV field");
            }
            in_quotes = true;
            at_field_start = false;
        } else {
            field.push_back(ch);
            at_field_start = false;
        }
    }
}

template <typename Integer>
Integer parseInteger(const std::string& text, const std::string& label, size_t row_number) {
    Integer value{};
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
        throw std::runtime_error("market_data.csv row " + std::to_string(row_number) +
                                 ": invalid " + label);
    }
    return value;
}

class MarketDataCsvReader {
public:
    explicit MarketDataCsvReader(std::istream& input) : input_(input) {
        const auto header = readRecord();
        const std::vector<std::string> expected = {
            "recv_tsec", "recv_tnsec", "venue", "stream_kind", "shard_id",
            "conn_epoch", "conn_seq", "symbol", "payload_json"
        };
        if (header != expected) {
            throw std::runtime_error("market_data.csv row 1: incorrect 9-column header");
        }
    }

    bool next(NormalizedMarketEvent& event) {
        const auto fields = readRecord();
        if (fields.empty() && input_.eof()) {
            return false;
        }
        ++record_number_;
        if (fields.size() != 9) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": expected 9 columns, found " + std::to_string(fields.size()));
        }

        event.recv_tsec = parseInteger<int64_t>(fields[0], "recv_tsec", record_number_);
        const int64_t recv_tnsec = parseInteger<int64_t>(fields[1], "recv_tnsec", record_number_);
        if (recv_tnsec < 0 || recv_tnsec > 999999999) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": recv_tnsec outside [0, 999999999]");
        }
        event.recv_tnsec = static_cast<int32_t>(recv_tnsec);
        event.venue = fields[2];
        if (event.venue != "spot" && event.venue != "usdm") {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": unsupported venue");
        }
        event.stream_kind = fields[3];
        if (event.stream_kind != "depth_diff" && event.stream_kind != "depth5" && event.stream_kind != "trade") {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": unsupported stream_kind " + event.stream_kind);
        }
        event.shard_id = parseInteger<int64_t>(fields[4], "shard_id", record_number_);
        if (event.shard_id < 0) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": shard_id must be non-negative");
        }
        event.conn_epoch = parseInteger<int64_t>(fields[5], "conn_epoch", record_number_);
        if (event.conn_epoch < 0) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": conn_epoch must be non-negative");
        }
        event.conn_seq = parseInteger<uint64_t>(fields[6], "conn_seq", record_number_);
        if (event.conn_seq == 0) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": conn_seq must be positive");
        }
        event.symbol = fields[7];
        if (event.symbol.empty() || normalizeSymbol(event.symbol) != event.symbol) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": symbol must be uppercase and non-empty");
        }
        try {
            event.payload = nlohmann::json::parse(fields[8]);
        } catch (const std::exception& exc) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": malformed payload_json: " + exc.what());
        }
        if (!event.payload.is_object()) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": payload_json must be an object");
        }

        validateMetadata(event);
        return true;
    }

    size_t rowNumber() const {
        return record_number_;
    }

private:
    std::vector<std::string> readRecord() {
        ++record_number_for_read_;
        return parseCsvRecord(input_, physical_line_, record_number_for_read_);
    }

    void validateMetadata(const NormalizedMarketEvent& event) {
        if (!metadata_initialized_) {
            venue_ = event.venue;
            symbol_ = event.symbol;
            shard_id_ = event.shard_id;
            epoch_ = event.conn_epoch;
            if (event.conn_seq != 1) {
                throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                         ": first conn_seq must be 1");
            }
            metadata_initialized_ = true;
            last_conn_seq_ = event.conn_seq;
            return;
        }

        if (event.venue != venue_ || event.symbol != symbol_ || event.shard_id != shard_id_) {
            throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                     ": replay supports one venue, symbol, and shard per file");
        }
        if (event.conn_epoch == epoch_) {
            if (last_conn_seq_ == std::numeric_limits<uint64_t>::max() ||
                event.conn_seq != last_conn_seq_ + 1) {
                throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                         ": conn_seq is not contiguous within conn_epoch");
            }
        } else {
            if (event.conn_epoch < epoch_ || event.conn_seq != 1) {
                throw std::runtime_error("market_data.csv row " + std::to_string(record_number_) +
                                         ": invalid conn_epoch/conn_seq transition");
            }
            epoch_ = event.conn_epoch;
        }
        last_conn_seq_ = event.conn_seq;
    }

    std::istream& input_;
    size_t physical_line_ = 1;
    size_t record_number_ = 1;
    size_t record_number_for_read_ = 0;
    bool metadata_initialized_ = false;
    std::string venue_;
    std::string symbol_;
    int64_t shard_id_ = 0;
    int64_t epoch_ = 0;
    uint64_t last_conn_seq_ = 0;
};

std::vector<DepthLevel> parseDepthLevels(const nlohmann::json& payload,
                                         const std::string& field_name) {
    if (!payload.contains(field_name) || !payload[field_name].is_array()) {
        throw std::invalid_argument("missing or invalid depth array " + field_name);
    }
    std::vector<DepthLevel> result;
    const auto& levels = payload[field_name];
    result.reserve(levels.size());
    for (const auto& level : levels) {
        if (!level.is_array() || level.size() != 2 || !level[0].is_string() || !level[1].is_string()) {
            throw std::invalid_argument("malformed price/quantity entry in depth array " + field_name);
        }
        const auto price = scaledIntegerFromString(level[0].get<std::string>(), PRICE_SCALE, field_name + ".price");
        const auto quantity = scaledIntegerFromString(level[1].get<std::string>(), QTY_SCALE, field_name + ".qty");
        if (price <= 0 || quantity < 0) {
            throw std::invalid_argument("invalid non-positive price or negative quantity in depth array " + field_name);
        }
        result.push_back({price, quantity});
    }
    return result;
}

OrderBookRow makeOrderBookRow(const NormalizedMarketEvent& event,
                              const OrderBook& book,
                              uint64_t seq_no,
                              char type) {
    const auto top = book.topFive();
    OrderBookRow row;
    row.tsec = event.recv_tsec;
    row.tnsec = event.recv_tnsec;
    row.seqNo = seq_no;
    row.id = static_cast<int32_t>(stableInstrumentId(event.symbol));
    row.type = type;
    row.side = 'N';
    row.bid_prices = top.bid_prices;
    row.bid_sizes = top.bid_sizes;
    row.ask_prices = top.ask_prices;
    row.ask_sizes = top.ask_sizes;
    return row;
}

}  // namespace

OrderBookProcessor::OrderBookProcessor(std::string venue,
                                       std::string symbol,
                                       OrderBookRecoveryMode recovery_mode,
                                       size_t buffer_limit)
    : venue_(std::move(venue)),
      symbol_(normalizeSymbol(symbol)),
      recovery_mode_(recovery_mode),
      buffer_limit_(buffer_limit) {
    if (venue_ != "spot" && venue_ != "usdm") {
        throw std::invalid_argument("venue must be spot or usdm");
    }
    if (symbol_.empty()) {
        throw std::invalid_argument("symbol must be non-empty");
    }
    if (buffer_limit_ == 0) {
        throw std::invalid_argument("resync buffer limit must be positive");
    }
}

ProcessedMarketEvent OrderBookProcessor::process(const NormalizedMarketEvent& event) {
    if (event.venue != venue_ || normalizeSymbol(event.symbol) != symbol_) {
        throw std::invalid_argument("market event venue/symbol does not match order-book processor");
    }
    if (event.recv_tnsec < 0 || event.recv_tnsec > 999999999) {
        throw std::invalid_argument("market event recv_tnsec outside [0, 999999999]");
    }
    if (!event.payload.is_object()) {
        throw std::invalid_argument("market event payload must be a JSON object");
    }

    if (event.stream_kind == "trade") {
        return {EventDisposition::trade, std::nullopt};
    }

    if (event.stream_kind == "depth_diff") {
        const auto bids = parseDepthLevels(event.payload, "b");
        const auto asks = parseDepthLevels(event.payload, "a");
        if (resynchronizing_) {
            uint64_t checked_seq = last_depth_seq_;
            bool checked_initial = initial_depth_diff_pending_;
            (void)applyDepthSequence(venue_, event.payload, checked_seq, checked_initial);
            if (buffered_depth_events_.size() >= buffer_limit_) {
                buffered_depth_events_.clear();
                return {EventDisposition::buffer_overflow, std::nullopt};
            }
            buffered_depth_events_.push_back(event);
            return {EventDisposition::buffered, std::nullopt};
        }
        if (book_out_of_sync_ || !have_depth_baseline_) {
            uint64_t checked_seq = last_depth_seq_;
            bool checked_initial = initial_depth_diff_pending_;
            (void)applyDepthSequence(venue_, event.payload, checked_seq, checked_initial);
            return {EventDisposition::ignored_out_of_sync, std::nullopt};
        }
        const auto sequence_status = applyDepthSequence(venue_, event.payload, last_depth_seq_, initial_depth_diff_pending_);
        if (sequence_status == DepthSequenceStatus::gap) {
            if (recovery_mode_ == OrderBookRecoveryMode::rest_snapshot) {
                beginResynchronization();
                buffered_depth_events_.push_back(event);
                return {EventDisposition::gap, std::nullopt};
            }
            book_out_of_sync_ = true;
            have_depth_baseline_ = false;
            initial_depth_diff_pending_ = false;
            order_book_.out_of_sync = true;
            return {EventDisposition::gap, std::nullopt};
        }
        if (sequence_status == DepthSequenceStatus::stale) {
            return {EventDisposition::stale_diff, std::nullopt};
        }
        order_book_.applyDepthDiff(bids, asks);
        ++orderbook_seq_;
        return {EventDisposition::applied_diff,
                makeOrderBookRow(event, order_book_, orderbook_seq_, 'D')};
    }

    if (event.stream_kind == "depth5") {
        const auto& bid_field = venue_ == "spot" ? "bids" : "b";
        const auto& ask_field = venue_ == "spot" ? "asks" : "a";
        const auto bids = parseDepthLevels(event.payload, bid_field);
        const auto asks = parseDepthLevels(event.payload, ask_field);
        const uint64_t snapshot_id = depthSnapshotUpdateId(venue_, event.payload);
        if (recovery_mode_ == OrderBookRecoveryMode::rest_snapshot) {
            if (resynchronizing_ || !have_depth_baseline_ || snapshot_id < last_depth_seq_) {
                return {EventDisposition::depth5_observed, std::nullopt};
            }
            const auto current_top = order_book_.topFive();
            bool matches = bids.size() <= current_top.bid_prices.size() &&
                           asks.size() <= current_top.ask_prices.size();
            for (size_t index = 0; matches && index < bids.size(); ++index) {
                matches = bids[index].price == current_top.bid_prices[index] &&
                          bids[index].qty == current_top.bid_sizes[index];
            }
            for (size_t index = 0; matches && index < asks.size(); ++index) {
                matches = asks[index].price == current_top.ask_prices[index] &&
                          asks[index].qty == current_top.ask_sizes[index];
            }
            if (!matches) {
                return {EventDisposition::depth5_mismatch, std::nullopt};
            }
            ++orderbook_seq_;
            return {EventDisposition::depth5_observed,
                    makeOrderBookRow(event, order_book_, orderbook_seq_, 'S')};
        }
        order_book_.applyDepth5(bids, asks);
        last_depth_seq_ = snapshot_id;
        have_depth_baseline_ = true;
        initial_depth_diff_pending_ = true;
        book_out_of_sync_ = false;
        order_book_.out_of_sync = false;
        ++orderbook_seq_;
        return {EventDisposition::snapshot,
                makeOrderBookRow(event, order_book_, orderbook_seq_, 'S')};
    }

    throw std::invalid_argument("unsupported stream_kind " + event.stream_kind);
}

void OrderBookProcessor::reset() {
    order_book_.bids.clear();
    order_book_.asks.clear();
    order_book_.out_of_sync = false;
    last_depth_seq_ = 0;
    have_depth_baseline_ = false;
    initial_depth_diff_pending_ = false;
    book_out_of_sync_ = true;
    resynchronizing_ = false;
    buffered_depth_events_.clear();
}

void OrderBookProcessor::beginResynchronization() {
    if (recovery_mode_ != OrderBookRecoveryMode::rest_snapshot) {
        throw std::logic_error("REST resynchronization is not enabled for this processor");
    }
    reset();
    resynchronizing_ = true;
}

bool OrderBookProcessor::isResynchronizing() const {
    return resynchronizing_;
}

size_t OrderBookProcessor::bufferedDepthEventCount() const {
    return buffered_depth_events_.size();
}

RestDepthSnapshot parseRestDepthSnapshot(const std::string& venue, const nlohmann::json& payload) {
    if (venue != "spot" && venue != "usdm") {
        throw std::invalid_argument("venue must be spot or usdm");
    }
    if (!payload.is_object()) {
        throw std::invalid_argument("REST depth snapshot must be a JSON object");
    }
    RestDepthSnapshot snapshot;
    snapshot.last_update_id = parseSequenceValue(payload, "lastUpdateId");
    snapshot.bids = parseDepthLevels(payload, "bids");
    snapshot.asks = parseDepthLevels(payload, "asks");
    return snapshot;
}

SnapshotRecoveryResult OrderBookProcessor::restoreFromRestSnapshot(const nlohmann::json& payload) {
    if (recovery_mode_ != OrderBookRecoveryMode::rest_snapshot || !resynchronizing_) {
        throw std::logic_error("REST snapshot received while processor is not resynchronizing");
    }
    const auto snapshot = parseRestDepthSnapshot(venue_, payload);
    SnapshotRecoveryResult result;
    result.snapshot_update_id = snapshot.last_update_id;
    result.buffered_events = buffered_depth_events_.size();

    OrderBook staged_book;
    staged_book.applyDepth5(snapshot.bids, snapshot.asks);
    uint64_t staged_last_update_id = snapshot.last_update_id;
    bool staged_initial_update_pending = true;
    uint64_t staged_orderbook_seq = orderbook_seq_;
    result.rows.reserve(buffered_depth_events_.size());

    for (const auto& event : buffered_depth_events_) {
        const auto bids = parseDepthLevels(event.payload, "b");
        const auto asks = parseDepthLevels(event.payload, "a");
        const auto status = applyDepthSequence(venue_, event.payload, staged_last_update_id, staged_initial_update_pending);
        if (status == DepthSequenceStatus::stale) {
            ++result.stale_events_discarded;
            continue;
        }
        if (status == DepthSequenceStatus::gap) {
            result.reason = "buffered differential events do not bridge snapshot sequence";
            result.rows.clear();
            return result;
        }
        staged_book.applyDepthDiff(bids, asks);
        ++staged_orderbook_seq;
        result.rows.push_back(makeOrderBookRow(event, staged_book, staged_orderbook_seq, 'D'));
    }

    order_book_ = std::move(staged_book);
    order_book_.out_of_sync = false;
    last_depth_seq_ = staged_last_update_id;
    orderbook_seq_ = staged_orderbook_seq;
    have_depth_baseline_ = true;
    initial_depth_diff_pending_ = staged_initial_update_pending;
    book_out_of_sync_ = false;
    resynchronizing_ = false;
    buffered_depth_events_.clear();
    result.synchronized = true;
    return result;
}

uint32_t restRetryDelaySeconds(uint32_t failed_attempt, uint32_t retry_after_seconds) {
    const uint32_t exponent = std::min(failed_attempt > 0 ? failed_attempt - 1 : 0, 4u);
    const uint32_t backoff = std::min(2u << exponent, 30u);
    return std::max(backoff, retry_after_seconds);
}

void writeOrderBookCsvHeader(std::ostream& output) {
    output << buildOrderBookHeader() << '\n';
}

void writeOrderBookCsvRow(std::ostream& output, const OrderBookRow& row) {
    output << row.tsec << ',' << row.tnsec << ',' << row.seqNo << ',' << row.id << ',' << row.type << ',' << row.side;
    for (const auto value : row.bid_prices) output << ',' << value;
    for (const auto value : row.bid_sizes) output << ',' << value;
    for (const auto value : row.ask_prices) output << ',' << value;
    for (const auto value : row.ask_sizes) output << ',' << value;
    output << '\n';
    if (!output) {
        throw std::runtime_error("failed writing order_book.csv");
    }
}

ReplayStats replayMarketDataCsv(std::istream& input,
                                std::ostream& order_book_output,
                                MetricsCollector* metrics) {
    MarketDataCsvReader reader(input);
    writeOrderBookCsvHeader(order_book_output);
    ReplayStats stats;
    std::optional<OrderBookProcessor> processor;
    int64_t previous_epoch = -1;
    NormalizedMarketEvent event;

    while (reader.next(event)) {
        ++stats.market_events;
        if (event.stream_kind == "depth_diff") ++stats.depth_diff_events;
        else if (event.stream_kind == "depth5") ++stats.depth5_events;
        else ++stats.trade_events;

        if (!processor) {
            processor.emplace(event.venue, event.symbol);
        } else if (event.conn_epoch != previous_epoch) {
            processor->reset();
        }
        previous_epoch = event.conn_epoch;

        ProcessedMarketEvent result;
        try {
            result = processor->process(event);
        } catch (const std::exception& exc) {
            throw std::runtime_error("market_data.csv row " + std::to_string(reader.rowNumber()) +
                                     ": " + exc.what());
        }
        if (metrics != nullptr) {
            metrics->observeEvent(event, result);
        }
        switch (result.disposition) {
            case EventDisposition::applied_diff: ++stats.applied_diff_events; break;
            case EventDisposition::stale_diff: ++stats.stale_diff_events; break;
            case EventDisposition::gap: ++stats.gap_events; break;
            case EventDisposition::ignored_out_of_sync: ++stats.ignored_out_of_sync_events; break;
            case EventDisposition::buffered:
            case EventDisposition::buffer_overflow:
            case EventDisposition::depth5_observed: break;
            case EventDisposition::depth5_mismatch: break;
            case EventDisposition::snapshot:
            case EventDisposition::trade: break;
        }
        if (result.order_book_row) {
            writeOrderBookCsvRow(order_book_output, *result.order_book_row);
            ++stats.order_book_rows;
        }
    }

    return stats;
}

}  // namespace binance_capture
