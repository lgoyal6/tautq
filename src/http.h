#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "loop.h"

namespace tautq {

// Hand-rolled HTTP/1.1 server on the epoll loop (D8: no framework). Deliberately small:
// request line + headers + Content-Length bodies (no chunked encoding, no pipelining),
// keep-alive honored, async responses supported — a handler may hold its Respond and call
// it after a quorum completes; the connection stays parked until then. Grant metadata
// travels in X-Tautq-* response headers so the worker needs no JSON parser.
class HttpServer {
  public:
    struct Request {
        std::string method;
        std::string path; // without query string
        std::map<std::string, std::string> query;
        std::map<std::string, std::string> headers; // lowercased names
        std::string body;
    };
    using Header = std::pair<std::string, std::string>;
    // Thread-free single-loop server: Respond must be called exactly once per request,
    // from the loop thread. Calling it after the client disconnected is a safe no-op.
    using Respond = std::function<void(int code, const std::string& content_type,
                                       const std::string& body, const std::vector<Header>& extra)>;
    using Handler = std::function<void(const Request&, Respond)>;

    explicit HttpServer(Loop& loop) : loop_(loop) {}
    ~HttpServer();

    bool listen(const std::string& addr, std::uint16_t port);
    // Longest-prefix routing on "METHOD path-prefix".
    void route(const std::string& method, const std::string& prefix, Handler h);

    std::uint16_t port() const {
        return port_;
    }

  private:
    struct Conn {
        int fd = -1;
        std::uint64_t gen = 0;
        std::string in;
        std::string out;
        bool close_after_write = false;
        bool awaiting_handler = false;
    };

    void accept_ready();
    void conn_ready(std::uint64_t gen, std::uint32_t events);
    // All of these take the shared_ptr BY VALUE: they may erase the connection from
    // conns_, and a reference into that map would die under the caller's feet.
    void try_parse(std::shared_ptr<Conn> c);
    void send_response(std::shared_ptr<Conn> c, bool keep_alive, int code,
                       const std::string& content_type, const std::string& body,
                       const std::vector<Header>& extra);
    void flush(std::shared_ptr<Conn> c);
    void close_conn(std::shared_ptr<Conn> c);

    Loop& loop_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::uint64_t next_gen_ = 1;
    std::unordered_map<std::uint64_t, std::shared_ptr<Conn>> conns_;
    std::vector<std::pair<std::string, Handler>> routes_; // "METHOD prefix" -> handler
};

} // namespace tautq
