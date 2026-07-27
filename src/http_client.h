#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace tautq {

// Tiny blocking HTTP/1.1 client for the worker/loadgen processes (each is single-purpose;
// blocking-with-timeout is the honest simple design there — the NODE never blocks).
struct HttpResponse {
    int code = 0;                               // 0 = transport failure / timeout
    std::map<std::string, std::string> headers; // lowercased names
    std::string body;
};

// url: http://ip:port/path (IPv4 only, like the rest of v1).
HttpResponse http_fetch(const std::string& method, const std::string& url,
                        const std::vector<std::pair<std::string, std::string>>& headers,
                        const std::string& body, std::chrono::milliseconds timeout);

} // namespace tautq
