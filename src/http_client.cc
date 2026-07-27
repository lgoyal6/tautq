#include "http_client.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace tautq {

namespace {

using Clock = std::chrono::steady_clock;

std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

int remaining_ms(Clock::time_point deadline) {
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    return ms < 0 ? 0 : static_cast<int>(ms);
}

bool wait_fd(int fd, short events, Clock::time_point deadline) {
    pollfd p{fd, events, 0};
    return ::poll(&p, 1, remaining_ms(deadline)) > 0;
}

} // namespace

HttpResponse http_fetch(const std::string& method, const std::string& url,
                        const std::vector<std::pair<std::string, std::string>>& headers,
                        const std::string& body, std::chrono::milliseconds timeout) {
    HttpResponse r;
    if (url.rfind("http://", 0) != 0) {
        return r;
    }
    const std::string rest = url.substr(7);
    const std::size_t slash = rest.find('/');
    const std::string hostport = slash == std::string::npos ? rest : rest.substr(0, slash);
    const std::string path = slash == std::string::npos ? "/" : rest.substr(slash);
    const std::size_t colon = hostport.find(':');
    const std::string host = colon == std::string::npos ? hostport : hostport.substr(0, colon);
    const std::uint16_t port = static_cast<std::uint16_t>(
        colon == std::string::npos ? 80 : std::strtoul(hostport.c_str() + colon + 1, nullptr, 10));

    const auto deadline = Clock::now() + timeout;
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        return r;
    }
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
        ::close(fd);
        return r;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0 && errno != EINPROGRESS) {
        ::close(fd);
        return r;
    }
    if (!wait_fd(fd, POLLOUT, deadline)) {
        ::close(fd);
        return r;
    }
    int err = 0;
    socklen_t elen = sizeof err;
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen) != 0 || err != 0) {
        ::close(fd);
        return r;
    }

    std::string req = method + " " + path + " HTTP/1.1\r\nHost: " + hostport +
                      "\r\nConnection: close\r\nContent-Length: " + std::to_string(body.size()) +
                      "\r\n";
    for (const auto& [k, v] : headers) {
        req += k + ": " + v + "\r\n";
    }
    req += "\r\n";
    req += body;

    std::size_t sent = 0;
    while (sent < req.size()) {
        const ssize_t n = ::send(fd, req.data() + sent, req.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!wait_fd(fd, POLLOUT, deadline)) {
                ::close(fd);
                return r;
            }
            continue;
        }
        ::close(fd);
        return r;
    }

    std::string resp;
    for (;;) {
        char buf[16 << 10];
        const ssize_t n = ::recv(fd, buf, sizeof buf, 0);
        if (n > 0) {
            resp.append(buf, static_cast<std::size_t>(n));
            continue;
        }
        if (n == 0) {
            break; // server closed (Connection: close)
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (!wait_fd(fd, POLLIN, deadline)) {
                ::close(fd);
                return r; // timed out mid-response
            }
            continue;
        }
        ::close(fd);
        return r;
    }
    ::close(fd);

    const std::size_t hdr_end = resp.find("\r\n\r\n");
    if (hdr_end == std::string::npos || resp.size() < 12) {
        return r;
    }
    r.code = std::atoi(resp.c_str() + 9);
    std::size_t pos = resp.find("\r\n") + 2;
    while (pos < hdr_end) {
        std::size_t eol = resp.find("\r\n", pos);
        if (eol == std::string::npos || eol > hdr_end) {
            eol = hdr_end;
        }
        const std::string line = resp.substr(pos, eol - pos);
        const std::size_t c = line.find(':');
        if (c != std::string::npos) {
            std::size_t vs = c + 1;
            while (vs < line.size() && line[vs] == ' ') {
                ++vs;
            }
            r.headers[lower(line.substr(0, c))] = line.substr(vs);
        }
        pos = eol + 2;
    }
    r.body = resp.substr(hdr_end + 4);
    return r;
}

} // namespace tautq
