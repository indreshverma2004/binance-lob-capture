#pragma once

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

namespace binance_capture {

class RestRequestError : public std::runtime_error {
public:
    RestRequestError(std::string message, uint32_t retry_after_seconds = 0);

    uint32_t retryAfterSeconds() const noexcept;

private:
    uint32_t retry_after_seconds_;
};

struct RestDepthRequest {
    std::string host;
    std::string target;
};

struct RestHttpResponse {
    uint32_t status = 0;
    std::string body;
    uint32_t retry_after_seconds = 0;
};

using RestHttpTransport = std::function<RestHttpResponse(const RestDepthRequest& request)>;

RestDepthRequest buildRestDepthRequest(const std::string& venue, const std::string& symbol);
nlohmann::json parseRestDepthResponse(const std::string& venue, const RestHttpResponse& response);
nlohmann::json fetchRestDepthSnapshot(const std::string& venue,
                                      const std::string& symbol,
                                      const RestHttpTransport& transport);
nlohmann::json fetchRestDepthSnapshot(const std::string& venue, const std::string& symbol);

}  // namespace binance_capture