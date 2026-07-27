// tautq-node: one cluster member. Wires taut (RealUdpTransport x2, Swim) + the queue
// protocol (QueueNode) + the admin/metrics HTTP server onto one epoll loop.
//
//   tautq-node --listen 10.9.0.1:9000 --http-port 8080 --data-dir /var/lib/tautq \
//              --peers 10.9.0.2:9000,10.9.0.3:9000 [--visibility-ms 30000]
//
// Port convention: SWIM rides on data-port + 1. Peers are data endpoints.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <sys/epoll.h>

#include "taut/swim.h"
#include "taut/transport.h"

#include "http.h"
#include "loop.h"
#include "metrics.h"
#include "queue_node.h"

using namespace tautq;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) {
    g_stop = 1;
}

std::string ep_str(const taut::Endpoint& e) {
    char buf[32];
    const auto a = ntohl(e.addr_be);
    std::snprintf(buf, sizeof buf, "%u.%u.%u.%u:%u", (a >> 24) & 0xFF, (a >> 16) & 0xFF,
                  (a >> 8) & 0xFF, a & 0xFF, ntohs(e.port_be));
    return buf;
}

std::optional<taut::Endpoint> parse_ep(const std::string& s) {
    const std::size_t colon = s.find(':');
    if (colon == std::string::npos) {
        return std::nullopt;
    }
    return taut::make_endpoint(s.substr(0, colon),
                               static_cast<std::uint16_t>(std::stoul(s.substr(colon + 1))));
}

taut::Endpoint swim_ep(const taut::Endpoint& data) {
    taut::Endpoint e = data;
    e.port_be = htons(static_cast<std::uint16_t>(ntohs(data.port_be) + 1));
    return e;
}
taut::Endpoint data_ep(const taut::Endpoint& swim) {
    taut::Endpoint e = swim;
    e.port_be = htons(static_cast<std::uint16_t>(ntohs(swim.port_be) - 1));
    return e;
}

// Ring/replica liveness backed by taut's SWIM (data-plane endpoints; SWIM speaks port+1).
class SwimMembership : public Membership {
  public:
    SwimMembership(taut::Swim& swim, taut::Endpoint self_data) : swim_(swim), self_(self_data) {}

    std::vector<taut::Endpoint> alive() const override {
        std::vector<taut::Endpoint> out{self_};
        for (const auto& [ep, st] : swim_.snapshot()) {
            if (st == taut::MemberState::Alive) {
                out.push_back(data_ep(ep));
            }
        }
        return out;
    }
    bool is_alive(const taut::Endpoint& e) const override {
        if (e == self_) {
            return true;
        }
        return swim_.state_of(swim_ep(e)) == taut::MemberState::Alive;
    }

  private:
    taut::Swim& swim_;
    taut::Endpoint self_;
};

const char* state_str(JobState s) {
    switch (s) {
    case JobState::Ready:
        return "ready";
    case JobState::Leased:
        return "leased";
    case JobState::Done:
        return "done";
    case JobState::DeadLetter:
        return "dead_letter";
    }
    return "?";
}

std::uint32_t qnum(const HttpServer::Request& r, const std::string& k, std::uint32_t dflt) {
    auto it = r.query.find(k);
    return it == r.query.end() ? dflt : static_cast<std::uint32_t>(std::stoul(it->second));
}

int http_code(std::uint32_t st) {
    if (st == qstatus::kCreated) {
        return 200;
    }
    if (st == qstatus::kDuplicate) {
        return 200;
    }
    if (st == qstatus::kInvalid) {
        return 400;
    }
    if (st == qstatus::kUnknownJob) {
        return 404;
    }
    if (st == qstatus::kNotLeased || st == qstatus::kNotOwner || st == qstatus::kStaleEpoch) {
        return 409;
    }
    return 503; // kNoQuorum + transport-level failures: retryable
}

} // namespace

int main(int argc, char** argv) {
    std::string listen_s;
    std::string data_dir = "./tautq-data";
    std::string peers_s;
    std::uint16_t http_port = 8080;
    NodeConfig cfg;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--listen") {
            listen_s = next();
        } else if (a == "--http-port") {
            http_port = static_cast<std::uint16_t>(std::stoul(next()));
        } else if (a == "--data-dir") {
            data_dir = next();
        } else if (a == "--peers") {
            peers_s = next();
        } else if (a == "--visibility-ms") {
            cfg.default_visibility_ms = static_cast<std::uint32_t>(std::stoul(next()));
        } else if (a == "--max-attempts") {
            cfg.default_max_attempts = static_cast<std::uint32_t>(std::stoul(next()));
        } else {
            std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
            return 2;
        }
    }
    const auto self = parse_ep(listen_s);
    if (!self) {
        std::fprintf(stderr, "usage: tautq-node --listen ip:port --http-port N "
                             "--data-dir DIR --peers ip:port,...\n");
        return 2;
    }
    std::vector<taut::Endpoint> peers;
    std::size_t pos = 0;
    while (pos < peers_s.size()) {
        std::size_t comma = peers_s.find(',', pos);
        if (comma == std::string::npos) {
            comma = peers_s.size();
        }
        if (const auto p = parse_ep(peers_s.substr(pos, comma - pos))) {
            peers.push_back(*p);
        }
        pos = comma + 1;
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);

    const std::string ip = listen_s.substr(0, listen_s.find(':'));
    taut::RealUdpTransport data_sock;
    taut::RealUdpTransport swim_sock;
    if (!data_sock.bind(ip, ntohs(self->port_be)) ||
        !swim_sock.bind(ip, static_cast<std::uint16_t>(ntohs(self->port_be) + 1))) {
        std::fprintf(stderr, "bind failed on %s\n", listen_s.c_str());
        return 1;
    }

    std::random_device rd;
    const std::uint64_t boot = (static_cast<std::uint64_t>(rd()) << 32) ^ rd();

    taut::SwimConfig swim_cfg; // reference defaults: T=1s, ping 300ms, suspicion 3s, k=3
    taut::Swim swim(swim_sock, swim_ep(*self), swim_cfg, boot);
    SwimMembership membership(swim, *self);
    QueueNode queue(
        data_sock, membership,
        [&] {
            NodeConfig c = cfg;
            c.self = *self;
            c.data_dir = data_dir;
            return c;
        }(),
        boot);
    if (!queue.open()) {
        std::fprintf(stderr, "failed to open data dir %s\n", data_dir.c_str());
        return 1;
    }

    std::uint64_t churn = 0;
    swim.on_state_change([&](const taut::Endpoint& ep, taut::MemberState st) {
        churn++;
        std::fprintf(stderr, "[swim] %s -> %d\n", ep_str(data_ep(ep)).c_str(),
                     static_cast<int>(st));
        if (st == taut::MemberState::Dead) {
            queue.on_peer_dead(data_ep(ep));
        }
    });
    for (const auto& p : peers) {
        swim.add_member(swim_ep(p));
    }
    if (!peers.empty()) {
        swim.join(swim_ep(peers[0])); // rejoin path for restarts (taut >= 0.1.1)
    }

    Histogram latency;
    queue.on_done_latency([&](double s) { latency.observe(s); });

    Loop loop;
    HttpServer http(loop);
    if (!loop.ok() || !http.listen("0.0.0.0", http_port)) {
        std::fprintf(stderr, "http listen failed on %u\n", http_port);
        return 1;
    }
    loop.add(data_sock.fd(), EPOLLIN, [&](std::uint32_t) { queue.poll(); });
    loop.add(swim_sock.fd(), EPOLLIN, [&](std::uint32_t) { swim.poll(); });

    http.route("POST", "/v1/jobs", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        QueueNode::SubmitParams p;
        p.idem_key = r.query.count("key") != 0 ? r.query.at("key") : "";
        p.url = r.query.count("url") != 0 ? r.query.at("url") : "";
        p.visibility_ms = qnum(r, "visibility_ms", 0);
        p.max_attempts = qnum(r, "max_attempts", 0);
        p.body.resize(r.body.size());
        std::memcpy(p.body.data(), r.body.data(), r.body.size());
        queue.submit(std::move(p), [re](std::uint32_t st, const JobId& id) {
            const bool created = st == qstatus::kCreated;
            const bool dup = st == qstatus::kDuplicate;
            if (created || dup) {
                re(created ? 201 : 200, "application/json",
                   "{\"id\":\"" + to_hex(id) + "\",\"status\":\"" +
                       (created ? "created" : "duplicate") + "\"}\n",
                   {});
            } else {
                re(http_code(st), "application/json", "{\"error\":" + std::to_string(st) + "}\n",
                   {});
            }
        });
    });
    http.route("POST", "/v1/lease", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        const std::uint64_t worker = qnum(r, "worker", 0);
        queue.lease(worker, [re](std::uint32_t st, const QueueNode::LeaseGrant& g) {
            if (st == qstatus::kCreated) {
                std::string body(reinterpret_cast<const char*>(g.body.data()), g.body.size());
                re(200, "application/octet-stream", body,
                   {{"X-Tautq-Id", to_hex(g.id)},
                    {"X-Tautq-Epoch", std::to_string(g.epoch)},
                    {"X-Tautq-Seq", std::to_string(g.lease_seq)},
                    {"X-Tautq-Url", g.url},
                    {"X-Tautq-Key", g.idem_key},
                    {"X-Tautq-Attempt", std::to_string(g.attempt)},
                    {"X-Tautq-Visibility-Ms", std::to_string(g.visibility_ms)}});
            } else if (st == qstatus::kNoJob) {
                re(204, "text/plain", "", {});
            } else {
                re(http_code(st), "text/plain", "unavailable\n", {});
            }
        });
    });
    http.route("POST", "/v1/ack", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        JobId id;
        if (r.query.count("id") == 0 || !from_hex(r.query.at("id"), id)) {
            re(400, "text/plain", "bad id\n", {});
            return;
        }
        const bool ok = r.query.count("result") != 0 && r.query.at("result") == "ok";
        queue.ack(id, qnum(r, "epoch", 0), qnum(r, "seq", 0), ok, [re](std::uint32_t st) {
            re(http_code(st), "application/json", "{\"status\":" + std::to_string(st) + "}\n", {});
        });
    });
    http.route("GET", "/v1/jobs/", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        JobId id;
        if (!from_hex(r.path.substr(std::strlen("/v1/jobs/")), id)) {
            re(400, "text/plain", "bad id\n", {});
            return;
        }
        const Job* j = queue.store().find(id);
        if (j == nullptr) {
            re(404, "application/json", "{\"error\":\"unknown\"}\n", {});
            return;
        }
        re(200, "application/json",
           std::string("{\"id\":\"") + to_hex(id) + "\",\"state\":\"" + state_str(j->state) +
               "\",\"epoch\":" + std::to_string(j->epoch) + ",\"attempts\":" +
               std::to_string(j->attempts) + ",\"owner\":\"" + ep_str(j->owner) + "\"}\n",
           {});
    });
    http.route("GET", "/v1/nodes", [&](const HttpServer::Request&, HttpServer::Respond re) {
        std::string out = "[";
        bool first = true;
        for (const auto& e : membership.alive()) {
            out += std::string(first ? "" : ",") + "{\"node\":\"" + ep_str(e) + "\"}";
            first = false;
        }
        out += "]\n";
        re(200, "application/json", out, {});
    });
    http.route("POST", "/v1/drain", [&](const HttpServer::Request&, HttpServer::Respond re) {
        queue.drain([] { std::fprintf(stderr, "[drain] complete\n"); });
        re(202, "application/json", "{\"status\":\"draining\"}\n", {});
    });
    http.route("GET", "/healthz", [&](const HttpServer::Request&, HttpServer::Respond re) {
        re(200, "text/plain", "ok\n", {});
    });
    http.route("GET", "/metrics", [&](const HttpServer::Request&, HttpServer::Respond re) {
        std::string out;
        std::uint64_t ready = 0;
        std::uint64_t leased = 0;
        std::uint64_t done = 0;
        std::uint64_t dead = 0;
        for (const auto& [id, j] : queue.store().jobs()) {
            (void)id;
            if (!(j.owner == queue.self())) {
                continue;
            }
            ready += j.state == JobState::Ready ? 1 : 0;
            leased += j.state == JobState::Leased ? 1 : 0;
            done += j.state == JobState::Done ? 1 : 0;
            dead += j.state == JobState::DeadLetter ? 1 : 0;
        }
        render_gauge("tautq_queue_depth", "{state=\"ready\"}", static_cast<double>(ready), out);
        render_gauge("tautq_queue_depth", "{state=\"dead_letter\"}", static_cast<double>(dead),
                     out);
        render_gauge("tautq_jobs_done", "", static_cast<double>(done), out);
        render_gauge("tautq_inflight_leases", "", static_cast<double>(leased), out);
        render_gauge("tautq_replication_backlog", "", static_cast<double>(queue.repair_backlog()),
                     out);
        const auto& c = queue.counters();
        render_counter("tautq_submits_total", "", c.submits, out);
        render_counter("tautq_duplicates_total", "", c.duplicates, out);
        render_counter("tautq_leases_granted_total", "", c.leases_granted, out);
        render_counter("tautq_completions_total", "{result=\"ok\"}", c.completions_ok, out);
        render_counter("tautq_completions_total", "{result=\"fail\"}", c.completions_failed, out);
        render_counter("tautq_expirations_total", "", c.expirations, out);
        render_counter("tautq_deadletters_total", "", c.deadletters, out);
        render_counter("tautq_takeovers_total", "", c.takeovers, out);
        render_counter("tautq_fenced_stale_total", "", c.fenced_stale, out);
        render_counter("tautq_membership_churn_total", "", churn, out);
        latency.render("tautq_job_latency_seconds", out);
        re(200, "text/plain; version=0.0.4", out, {});
    });

    std::fprintf(stderr, "[tautq] node %s http=%u data-dir=%s peers=%zu\n", listen_s.c_str(),
                 http_port, data_dir.c_str(), peers.size());
    while (g_stop == 0) {
        if (!loop.run_once(std::chrono::milliseconds(10))) {
            break;
        }
        queue.poll();
        queue.tick();
        swim.poll();
        swim.tick();
    }
    std::fprintf(stderr, "[tautq] shutting down\n");
    return 0;
}
