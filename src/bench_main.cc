// tautq-bench: open-loop submit generator with per-request raw records, for the overload
// study. Unlike tautq-loadgen (thread-per-slice blocking client + histogram deltas), this is
// a single-threaded epoll client: every request's INTENDED send time comes from a seeded
// Poisson schedule fixed before the run, latency is measured from that intended time
// (coordinated omission corrected: a stalled server cannot slow the offered load or hide
// its own queueing), and every attempt is written out.
//
//   tautq-bench --gateways http://ip:port,... --sink-url http://ip:port/hook \
//               --phases 10:400,20:2400,10:400  --seed 7 --out raw.csv \
//               [--timeout-ms 3000] [--max-conns 6000] [--retries 3] [--payload 128]
//               [--prefix run]
//
// --phases is duration_s:rate_per_s,... (piecewise-constant Poisson rate).
// Retry contract: 429 (known refusal) is NOT retried here (it is the measured outcome);
// 503 / timeout / connection failure (outcome unknown or unavailable) is retried with the
// SAME key after 100ms, up to --retries times, so unknown outcomes resolve to one job.
//
// raw.csv columns: key,attempt,intended_us,sent_us,done_us,code,id   (steady-clock us)
// A request that cannot get a socket within its slot (fd/conn cap) is recorded with
// code -1 ("client_saturated") rather than silently delayed.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

std::int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now().time_since_epoch())
        .count();
}

struct Target {
    sockaddr_in addr{};
    std::string host;
};

bool parse_url(const std::string& url, Target& t, std::string& path) {
    const std::string pre = "http://";
    if (url.rfind(pre, 0) != 0)
        return false;
    const auto rest = url.substr(pre.size());
    const auto slash = rest.find('/');
    const auto hostport = rest.substr(0, slash);
    path = slash == std::string::npos ? "/" : rest.substr(slash);
    const auto colon = hostport.find(':');
    t.host = hostport;
    t.addr.sin_family = AF_INET;
    t.addr.sin_port = htons(static_cast<std::uint16_t>(
        colon == std::string::npos ? 80 : std::stoul(hostport.substr(colon + 1))));
    return ::inet_pton(AF_INET, hostport.substr(0, colon).c_str(), &t.addr.sin_addr) == 1;
}

struct Req {
    std::string key;
    int attempt = 0;
    std::int64_t intended_us = 0;
    std::int64_t sent_us = 0;
    std::size_t gw = 0;
};

struct Conn {
    int fd = -1;
    Req req;
    std::string out;
    std::size_t written = 0;
    std::string in;
    std::int64_t deadline_us = 0;
};

} // namespace

int main(int argc, char** argv) {
    std::string gateways_s;
    std::string sink_url;
    std::string phases_s = "10:100";
    std::string out_path = "bench.csv";
    std::string prefix = "b";
    std::uint64_t seed = 1;
    int timeout_ms = 3000;
    std::size_t max_conns = 6000;
    int retries = 3;
    std::size_t payload = 128;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--gateways")
            gateways_s = next();
        else if (a == "--sink-url")
            sink_url = next();
        else if (a == "--phases")
            phases_s = next();
        else if (a == "--out")
            out_path = next();
        else if (a == "--prefix")
            prefix = next();
        else if (a == "--seed")
            seed = std::stoull(next());
        else if (a == "--timeout-ms")
            timeout_ms = std::atoi(next().c_str());
        else if (a == "--max-conns")
            max_conns = std::stoul(next());
        else if (a == "--retries")
            retries = std::atoi(next().c_str());
        else if (a == "--payload")
            payload = std::stoul(next());
        else {
            std::fprintf(stderr, "unknown arg %s\n", a.c_str());
            return 2;
        }
    }
    std::vector<Target> gws;
    std::vector<std::string> gw_paths;
    for (std::size_t pos = 0; pos < gateways_s.size();) {
        auto comma = gateways_s.find(',', pos);
        if (comma == std::string::npos)
            comma = gateways_s.size();
        Target t;
        std::string path;
        if (!parse_url(gateways_s.substr(pos, comma - pos), t, path)) {
            std::fprintf(stderr, "bad gateway url\n");
            return 2;
        }
        gws.push_back(t);
        pos = comma + 1;
    }
    if (gws.empty() || sink_url.empty()) {
        std::fprintf(stderr,
                     "usage: tautq-bench --gateways URL,... --sink-url URL --phases d:r,...\n");
        return 2;
    }
    rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
        if (rl.rlim_cur < max_conns + 64)
            max_conns = rl.rlim_cur > 128 ? rl.rlim_cur - 64 : 64;
    }

    // ---- fixed schedule: piecewise Poisson ------------------------------------------------
    std::mt19937_64 rng(seed);
    std::deque<Req> schedule;
    const std::int64_t t0 = now_us() + 200000; // 200ms lead
    std::int64_t phase_start = t0;
    std::uint64_t seq = 0;
    for (std::size_t pos = 0; pos < phases_s.size();) {
        auto comma = phases_s.find(',', pos);
        if (comma == std::string::npos)
            comma = phases_s.size();
        const std::string ph = phases_s.substr(pos, comma - pos);
        pos = comma + 1;
        const auto colon = ph.find(':');
        const double dur_s = std::stod(ph.substr(0, colon));
        const double rate = std::stod(ph.substr(colon + 1));
        const std::int64_t end = phase_start + static_cast<std::int64_t>(dur_s * 1e6);
        if (rate > 0) {
            std::exponential_distribution<double> gap(rate);
            double t = static_cast<double>(phase_start);
            for (;;) {
                t += gap(rng) * 1e6;
                if (t >= static_cast<double>(end))
                    break;
                Req r;
                r.key = prefix + "-" + std::to_string(seq);
                r.intended_us = static_cast<std::int64_t>(t);
                r.gw = static_cast<std::size_t>(seq % gws.size());
                ++seq;
                schedule.push_back(std::move(r));
            }
        }
        phase_start = end;
    }
    const std::int64_t t_end = phase_start;
    std::fprintf(stderr, "bench: %zu requests scheduled over %.1fs\n", schedule.size(),
                 static_cast<double>(t_end - t0) / 1e6);

    std::FILE* out = std::fopen(out_path.c_str(), "w");
    if (out == nullptr)
        return 1;
    std::fprintf(out, "key,attempt,intended_us,sent_us,done_us,code,id\n");
    const std::string body(payload, 'x');

    const int ep = ::epoll_create1(EPOLL_CLOEXEC);
    std::unordered_map<int, std::unique_ptr<Conn>> conns;
    std::multimap<std::int64_t, Req> retry_q; // due_us -> req
    std::uint64_t completed = 0;
    std::uint64_t saturated = 0;

    const auto finish = [&](Conn& c, int code, const std::string& id) {
        const std::int64_t done = now_us();
        std::fprintf(out, "%s,%d,%" PRId64 ",%" PRId64 ",%" PRId64 ",%d,%s\n", c.req.key.c_str(),
                     c.req.attempt, c.req.intended_us, c.req.sent_us, done, code, id.c_str());
        ++completed;
        if (completed % 256 == 0) {
            std::fflush(out); // live progress for the demo; the file is complete at exit
        }
        const bool retryable = code == 0 || code == 503;
        if (retryable && c.req.attempt < retries) {
            Req r = c.req;
            r.attempt++;
            r.intended_us = done + 100000; // retry intent: 100ms after the failure
            retry_q.emplace(r.intended_us, r);
        }
        ::epoll_ctl(ep, EPOLL_CTL_DEL, c.fd, nullptr);
        ::close(c.fd);
    };

    const auto launch = [&](Req r) {
        r.sent_us = now_us();
        if (conns.size() >= max_conns) {
            std::fprintf(out, "%s,%d,%" PRId64 ",%" PRId64 ",%" PRId64 ",-1,\n", r.key.c_str(),
                         r.attempt, r.intended_us, r.sent_us, r.sent_us);
            ++saturated;
            return;
        }
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            std::fprintf(out, "%s,%d,%" PRId64 ",%" PRId64 ",%" PRId64 ",-1,\n", r.key.c_str(),
                         r.attempt, r.intended_us, r.sent_us, r.sent_us);
            ++saturated;
            return;
        }
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        const Target& t = gws[r.gw];
        auto c = std::make_unique<Conn>();
        c->fd = fd;
        c->deadline_us = r.sent_us + static_cast<std::int64_t>(timeout_ms) * 1000;
        const std::string path = "/v1/jobs?key=" + r.key + "&url=" + sink_url;
        c->out = "POST " + path + " HTTP/1.1\r\nHost: " + t.host +
                 "\r\nConnection: close\r\nContent-Length: " + std::to_string(body.size()) +
                 "\r\n\r\n" + body;
        c->req = std::move(r);
        const int rc = ::connect(fd, reinterpret_cast<const sockaddr*>(&t.addr), sizeof t.addr);
        if (rc != 0 && errno != EINPROGRESS) {
            finish(*c, 0, "");
            return;
        }
        epoll_event ev{};
        ev.events = EPOLLOUT | EPOLLIN | EPOLLRDHUP;
        ev.data.fd = fd;
        ::epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev);
        conns[fd] = std::move(c);
    };

    const auto parse_done = [](const std::string& in, int& code, std::string& id) {
        // "HTTP/1.1 201 Created\r\n...\r\n\r\n{"id":"..."}"
        if (in.size() < 12)
            return false;
        const auto hdr_end = in.find("\r\n\r\n");
        if (hdr_end == std::string::npos)
            return false;
        code = std::atoi(in.c_str() + 9);
        std::size_t clen = 0;
        const auto cl = in.find("Content-Length:");
        if (cl != std::string::npos && cl < hdr_end)
            clen = std::strtoul(in.c_str() + cl + 15, nullptr, 10);
        if (in.size() < hdr_end + 4 + clen)
            return false;
        const auto idp = in.find("\"id\":\"", hdr_end);
        if (idp != std::string::npos) {
            const auto e = in.find('"', idp + 6);
            id = in.substr(idp + 6, e - idp - 6);
        }
        return true;
    };

    std::vector<epoll_event> evs(512);
    for (;;) {
        const std::int64_t now = now_us();
        while (!schedule.empty() && schedule.front().intended_us <= now) {
            launch(std::move(schedule.front()));
            schedule.pop_front();
        }
        while (!retry_q.empty() && retry_q.begin()->first <= now) {
            Req r = retry_q.begin()->second;
            retry_q.erase(retry_q.begin());
            launch(std::move(r));
        }
        // Timeouts.
        std::vector<int> expired;
        for (const auto& [fd, c] : conns) {
            if (c->deadline_us <= now)
                expired.push_back(fd);
        }
        for (int fd : expired) {
            finish(*conns[fd], 0, "");
            conns.erase(fd);
        }
        if (schedule.empty() && retry_q.empty() && conns.empty())
            break;
        std::int64_t wait_us = 1000;
        if (!schedule.empty())
            wait_us = std::min<std::int64_t>(
                wait_us, std::max<std::int64_t>(0, schedule.front().intended_us - now));
        const int n = ::epoll_wait(ep, evs.data(), static_cast<int>(evs.size()),
                                   static_cast<int>(wait_us / 1000));
        for (int i = 0; i < n; ++i) {
            const int fd = evs[static_cast<std::size_t>(i)].data.fd;
            auto it = conns.find(fd);
            if (it == conns.end())
                continue;
            Conn& c = *it->second;
            const auto e = evs[static_cast<std::size_t>(i)].events;
            bool closed = false;
            if ((e & EPOLLOUT) != 0 && c.written < c.out.size()) {
                const ssize_t w =
                    ::send(fd, c.out.data() + c.written, c.out.size() - c.written, MSG_NOSIGNAL);
                if (w > 0) {
                    c.written += static_cast<std::size_t>(w);
                    if (c.written == c.out.size()) {
                        epoll_event ev{};
                        ev.events = EPOLLIN | EPOLLRDHUP;
                        ev.data.fd = fd;
                        ::epoll_ctl(ep, EPOLL_CTL_MOD, fd, &ev);
                    }
                } else if (w < 0 && errno != EAGAIN) {
                    finish(c, 0, "");
                    closed = true;
                }
            }
            if (!closed && (e & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0) {
                char buf[4096];
                for (;;) {
                    const ssize_t r = ::recv(fd, buf, sizeof buf, 0);
                    if (r > 0) {
                        c.in.append(buf, static_cast<std::size_t>(r));
                        continue;
                    }
                    int code = 0;
                    std::string id;
                    if (parse_done(c.in, code, id)) {
                        finish(c, code, id);
                        closed = true;
                    } else if (r == 0 || (r < 0 && errno != EAGAIN)) {
                        finish(c, 0, "");
                        closed = true;
                    }
                    break;
                }
                if (!closed) {
                    int code = 0;
                    std::string id;
                    if (parse_done(c.in, code, id)) {
                        finish(c, code, id);
                        closed = true;
                    }
                }
            }
            if (closed)
                conns.erase(fd);
        }
    }
    std::fclose(out);
    std::fprintf(stderr, "bench: done attempts=%" PRIu64 " client_saturated=%" PRIu64 "\n",
                 completed + saturated, saturated);
    return 0;
}
