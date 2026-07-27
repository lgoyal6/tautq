#include "http.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace tautq {

namespace {

constexpr std::size_t kMaxRequestBytes = 256 << 10;

bool set_nonblock(int fd) {
    const int fl = ::fcntl(fd, F_GETFL, 0);
    return fl >= 0 && ::fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0;
}

std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

std::string url_decode(const std::string& s) {
    std::string out;
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            const auto hex = [](char c) -> int {
                if (c >= '0' && c <= '9') {
                    return c - '0';
                }
                if (c >= 'a' && c <= 'f') {
                    return c - 'a' + 10;
                }
                if (c >= 'A' && c <= 'F') {
                    return c - 'A' + 10;
                }
                return -1;
            };
            const int hi = hex(s[i + 1]);
            const int lo = hex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>(hi * 16 + lo));
                i += 2;
                continue;
            }
        }
        out.push_back(s[i] == '+' ? ' ' : s[i]);
    }
    return out;
}

const char* reason(int code) {
    switch (code) {
    case 200:
        return "OK";
    case 201:
        return "Created";
    case 202:
        return "Accepted";
    case 204:
        return "No Content";
    case 400:
        return "Bad Request";
    case 404:
        return "Not Found";
    case 409:
        return "Conflict";
    case 429:
        return "Too Many Requests";
    case 500:
        return "Internal Server Error";
    case 503:
        return "Service Unavailable";
    default:
        return "OK";
    }
}

} // namespace

HttpServer::~HttpServer() {
    for (auto& [gen, c] : conns_) {
        (void)gen;
        if (c->fd >= 0) {
            loop_.del(c->fd);
            ::close(c->fd);
        }
    }
    if (listen_fd_ >= 0) {
        loop_.del(listen_fd_);
        ::close(listen_fd_);
    }
}

bool HttpServer::listen(const std::string& addr, std::uint16_t port) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        return false;
    }
    const int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) != 1) {
        return false;
    }
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0 ||
        ::listen(listen_fd_, 128) != 0 || !set_nonblock(listen_fd_)) {
        return false;
    }
    sockaddr_in got{};
    socklen_t len = sizeof got;
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&got), &len);
    port_ = ntohs(got.sin_port);
    return loop_.add(listen_fd_, EPOLLIN, [this](std::uint32_t) { accept_ready(); });
}

void HttpServer::route(const std::string& method, const std::string& prefix, Handler h) {
    routes_.emplace_back(method + " " + prefix, std::move(h));
    // Longest prefix first.
    std::sort(routes_.begin(), routes_.end(),
              [](const auto& a, const auto& b) { return a.first.size() > b.first.size(); });
}

void HttpServer::accept_ready() {
    for (;;) {
        const int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            return; // EAGAIN or transient
        }
        if (!set_nonblock(fd)) {
            ::close(fd);
            continue;
        }
        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        auto c = std::make_shared<Conn>();
        c->fd = fd;
        c->gen = next_gen_++;
        conns_[c->gen] = c;
        const std::uint64_t gen = c->gen;
        if (!loop_.add(fd, EPOLLIN, [this, gen](std::uint32_t ev) { conn_ready(gen, ev); })) {
            ::close(fd);
            conns_.erase(gen);
        }
    }
}

void HttpServer::conn_ready(std::uint64_t gen, std::uint32_t events) {
    auto it = conns_.find(gen);
    if (it == conns_.end()) {
        return;
    }
    std::shared_ptr<Conn> c = it->second;
    if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
        close_conn(c);
        return;
    }
    if ((events & EPOLLIN) != 0) {
        char buf[16 << 10];
        for (;;) {
            const ssize_t n = ::read(c->fd, buf, sizeof buf);
            if (n > 0) {
                c->in.append(buf, static_cast<std::size_t>(n));
                if (c->in.size() > kMaxRequestBytes) {
                    close_conn(c);
                    return;
                }
                continue;
            }
            if (n == 0) {
                close_conn(c);
                return;
            }
            break; // EAGAIN
        }
        if (!c->awaiting_handler) {
            try_parse(c);
        }
    }
    if ((events & EPOLLOUT) != 0) {
        flush(c);
    }
}

void HttpServer::try_parse(std::shared_ptr<Conn> c) {
    const std::size_t hdr_end = c->in.find("\r\n\r\n");
    if (hdr_end == std::string::npos) {
        return;
    }
    Request req;
    // Request line.
    const std::size_t line_end = c->in.find("\r\n");
    {
        const std::string line = c->in.substr(0, line_end);
        const std::size_t sp1 = line.find(' ');
        const std::size_t sp2 = line.rfind(' ');
        if (sp1 == std::string::npos || sp2 == sp1) {
            close_conn(c);
            return;
        }
        req.method = line.substr(0, sp1);
        std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        const std::size_t q = target.find('?');
        if (q != std::string::npos) {
            std::string qs = target.substr(q + 1);
            target.resize(q);
            std::size_t pos = 0;
            while (pos < qs.size()) {
                std::size_t amp = qs.find('&', pos);
                if (amp == std::string::npos) {
                    amp = qs.size();
                }
                const std::string kv = qs.substr(pos, amp - pos);
                const std::size_t eq = kv.find('=');
                if (eq != std::string::npos) {
                    req.query[url_decode(kv.substr(0, eq))] = url_decode(kv.substr(eq + 1));
                } else if (!kv.empty()) {
                    req.query[url_decode(kv)] = "";
                }
                pos = amp + 1;
            }
        }
        req.path = url_decode(target);
    }
    // Headers.
    std::size_t pos = line_end + 2;
    while (pos < hdr_end) {
        std::size_t eol = c->in.find("\r\n", pos);
        if (eol == std::string::npos || eol > hdr_end) {
            eol = hdr_end;
        }
        const std::string line = c->in.substr(pos, eol - pos);
        const std::size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string name = lower(line.substr(0, colon));
            std::size_t vstart = colon + 1;
            while (vstart < line.size() && line[vstart] == ' ') {
                ++vstart;
            }
            req.headers[name] = line.substr(vstart);
        }
        pos = eol + 2;
    }
    std::size_t content_len = 0;
    if (const auto clit = req.headers.find("content-length"); clit != req.headers.end()) {
        content_len = static_cast<std::size_t>(std::strtoul(clit->second.c_str(), nullptr, 10));
        if (content_len > kMaxRequestBytes) {
            close_conn(c);
            return;
        }
    }
    const std::size_t total = hdr_end + 4 + content_len;
    if (c->in.size() < total) {
        return; // body still arriving
    }
    req.body = c->in.substr(hdr_end + 4, content_len);
    c->in.erase(0, total);

    const bool keep_alive =
        lower(req.headers.count("connection") != 0 ? req.headers.at("connection") : "keep-alive") !=
        "close";

    // Route (longest prefix).
    Handler* handler = nullptr;
    const std::string key = req.method + " " + req.path;
    for (auto& [prefix, h] : routes_) {
        if (key.rfind(prefix, 0) == 0) {
            handler = &h;
            break;
        }
    }
    c->awaiting_handler = true;
    const std::uint64_t gen = c->gen;
    Respond respond = [this, gen, keep_alive](int code, const std::string& ct,
                                              const std::string& body,
                                              const std::vector<Header>& extra) {
        auto cit = conns_.find(gen);
        if (cit == conns_.end()) {
            return; // client went away while the handler worked — drop the response
        }
        send_response(cit->second, keep_alive, code, ct, body, extra);
    };
    if (handler == nullptr) {
        respond(404, "text/plain", "not found\n", {});
        return;
    }
    (*handler)(req, respond);
}

void HttpServer::send_response(std::shared_ptr<Conn> c, bool keep_alive, int code,
                               const std::string& content_type, const std::string& body,
                               const std::vector<Header>& extra) {
    char head[256];
    std::snprintf(head, sizeof head,
                  "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
                  "Connection: %s\r\n",
                  code, reason(code), content_type.c_str(), body.size(),
                  keep_alive ? "keep-alive" : "close");
    c->out += head;
    for (const auto& [k, v] : extra) {
        c->out += k;
        c->out += ": ";
        c->out += v;
        c->out += "\r\n";
    }
    c->out += "\r\n";
    c->out += body;
    c->close_after_write = !keep_alive;
    c->awaiting_handler = false;
    flush(c);
    // A pipelined/queued next request may already be buffered.
    auto it = conns_.find(c->gen);
    if (it != conns_.end() && !c->awaiting_handler && !c->in.empty()) {
        try_parse(c);
    }
}

void HttpServer::flush(std::shared_ptr<Conn> c) {
    while (!c->out.empty()) {
        const ssize_t n = ::write(c->fd, c->out.data(), c->out.size());
        if (n > 0) {
            c->out.erase(0, static_cast<std::size_t>(n));
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            loop_.mod(c->fd, EPOLLIN | EPOLLOUT);
            return;
        }
        close_conn(c);
        return;
    }
    loop_.mod(c->fd, EPOLLIN);
    if (c->close_after_write) {
        close_conn(c);
    }
}

void HttpServer::close_conn(std::shared_ptr<Conn> c) {
    if (c->fd >= 0) {
        loop_.del(c->fd);
        ::close(c->fd);
        c->fd = -1;
    }
    conns_.erase(c->gen);
}

} // namespace tautq
