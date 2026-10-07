#include "rest_snapshot_client.hpp"
#include "binance_capture.hpp"
#include "market_replay.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <utility>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

namespace binance_capture {

namespace {

void loadPlatformTrust(ssl::context& context);

std::string encodeQueryValue(const std::string& value) {
    static constexpr char digits[] = "0123456789ABCDEF";
    std::string encoded;
    for (unsigned char ch : value) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
            (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
            encoded.push_back(static_cast<char>(ch));
        } else {
            encoded.push_back('%');
            encoded.push_back(digits[ch >> 4]);
            encoded.push_back(digits[ch & 0x0F]);
        }
    }
    return encoded;
}

uint32_t retryAfterSeconds(const http::response<http::string_body>& response) {
    const auto value = response[http::field::retry_after];
    if (value.empty()) {
        return 0;
    }
    uint32_t seconds = 0;
    const auto result = std::from_chars(value.data(), value.data() + value.size(), seconds);
    return result.ec == std::errc{} && result.ptr == value.data() + value.size() ? seconds : 0;
}

RestHttpResponse performHttpGet(const RestDepthRequest& request) {
    asio::io_context io;
    ssl::context context(ssl::context::tls_client);
    loadPlatformTrust(context);
    tcp::resolver resolver(io);
    beast::ssl_stream<beast::tcp_stream> stream(io, context);
    stream.set_verify_mode(ssl::verify_peer);
    stream.set_verify_callback(ssl::host_name_verification(request.host));
    if (SSL_set_tlsext_host_name(stream.native_handle(), request.host.c_str()) != 1) {
        throw std::runtime_error("unable to set REST TLS SNI");
    }

    http::request<http::empty_body> http_request{http::verb::get, request.target, 11};
    http_request.set(http::field::host, request.host);
    http_request.set(http::field::user_agent, "binance_market_data_collector/1.0");
    http_request.set(http::field::accept, "application/json");
    http_request.keep_alive(false);
    beast::flat_buffer buffer;
    http::response_parser<http::string_body> parser;
    parser.body_limit(4 * 1024 * 1024);
    http::response<http::string_body> response;
    asio::steady_timer deadline(io);
    bool completed = false;
    bool timed_out = false;
    std::string request_error;
    auto finishWithError = [&](const boost::system::error_code& error) {
        if (error && !completed) {
            completed = true;
            request_error = error.message();
            deadline.cancel();
        }
    };

    deadline.expires_after(std::chrono::seconds(10));
    deadline.async_wait([&](const boost::system::error_code& error) {
        if (!error && !completed) {
            timed_out = true;
            completed = true;
            request_error = "request timed out after 10 seconds";
            resolver.cancel();
            boost::system::error_code ignored;
            beast::get_lowest_layer(stream).socket().cancel(ignored);
        }
    });

    resolver.async_resolve(request.host, "443",
        [&](const boost::system::error_code& resolve_error, tcp::resolver::results_type endpoints) {
            if (resolve_error) {
                finishWithError(resolve_error);
                return;
            }
            if (completed) return;
            beast::get_lowest_layer(stream).async_connect(endpoints,
                [&](const boost::system::error_code& connect_error, const tcp::endpoint&) {
                    if (connect_error) {
                        finishWithError(connect_error);
                        return;
                    }
                    if (completed) return;
                    stream.async_handshake(ssl::stream_base::client,
                        [&](const boost::system::error_code& handshake_error) {
                            if (handshake_error) {
                                finishWithError(handshake_error);
                                return;
                            }
                            if (completed) return;
                            http::async_write(stream, http_request,
                                [&](const boost::system::error_code& write_error, size_t) {
                                    if (write_error) {
                                        finishWithError(write_error);
                                        return;
                                    }
                                    if (completed) return;
                                    http::async_read(stream, buffer, parser,
                                        [&](const boost::system::error_code& read_error, size_t) {
                                            if (read_error) {
                                                finishWithError(read_error);
                                                return;
                                            }
                                            if (completed) return;
                                            response = parser.release();
                                            completed = true;
                                            deadline.cancel();
                                        });
                                });
                        });
                });
        });
    io.run();
    if (timed_out) {
        throw std::runtime_error(request_error);
    }
    if (!completed || !request_error.empty()) {
        throw std::runtime_error("HTTP request failed: " + request_error);
    }
    const auto status = response.result_int();
    const auto retry_after = (status == 429 || status == 418) ? retryAfterSeconds(response) : 0;
    return {status, response.body(), retry_after};
}

void loadPlatformTrust(ssl::context& context) {
    context.set_default_verify_paths();
    if (std::filesystem::exists("C:/msys64/usr/ssl/certs/ca-bundle.crt")) {
        context.load_verify_file("C:/msys64/usr/ssl/certs/ca-bundle.crt");
    } else if (std::filesystem::exists("C:/msys64/usr/ssl/cert.pem")) {
        context.load_verify_file("C:/msys64/usr/ssl/cert.pem");
    }
}

}  // namespace

RestRequestError::RestRequestError(std::string message, uint32_t retry_after_seconds)
    : std::runtime_error(std::move(message)), retry_after_seconds_(retry_after_seconds) {}

uint32_t RestRequestError::retryAfterSeconds() const noexcept {
    return retry_after_seconds_;
}

RestDepthRequest buildRestDepthRequest(const std::string& venue, const std::string& symbol) {
    const std::string uppercase_symbol = normalizeSymbol(symbol);
    if (venue == "spot") {
        return {"api.binance.com", "/api/v3/depth?symbol=" + encodeQueryValue(uppercase_symbol) + "&limit=1000"};
    }
    if (venue == "usdm") {
        return {"fapi.binance.com", "/fapi/v1/depth?symbol=" + encodeQueryValue(uppercase_symbol) + "&limit=1000"};
    }
    throw std::invalid_argument("venue must be spot or usdm");
}

nlohmann::json parseRestDepthResponse(const std::string& venue, const RestHttpResponse& response) {
    if (response.status != 200) {
        const auto retry_after = (response.status == 429 || response.status == 418)
                                     ? response.retry_after_seconds
                                     : 0;
        throw RestRequestError("REST snapshot HTTP " + std::to_string(response.status) +
                                   (response.body.empty() ? "" : ": " + response.body.substr(0, 512)),
                               retry_after);
    }
    try {
        const auto payload = nlohmann::json::parse(response.body);
        (void)parseRestDepthSnapshot(venue, payload);
        return payload;
    } catch (const RestRequestError&) {
        throw;
    } catch (const std::exception& exc) {
        throw RestRequestError("malformed REST snapshot response for " + venue + ": " + exc.what());
    }
}

nlohmann::json fetchRestDepthSnapshot(const std::string& venue,
                                      const std::string& symbol,
                                      const RestHttpTransport& transport) {
    if (!transport) {
        throw std::invalid_argument("REST HTTP transport is empty");
    }
    const auto request = buildRestDepthRequest(venue, symbol);
    try {
        return parseRestDepthResponse(venue, transport(request));
    } catch (const RestRequestError&) {
        throw;
    } catch (const std::exception& exc) {
        throw RestRequestError("REST snapshot transport failed for " + venue + " " + normalizeSymbol(symbol) +
                               ": " + exc.what());
    }
}

nlohmann::json fetchRestDepthSnapshot(const std::string& venue, const std::string& symbol) {
    return fetchRestDepthSnapshot(venue, symbol, performHttpGet);
}

}  // namespace binance_capture