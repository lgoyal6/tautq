#pragma once

// Shared test harness: an N-node tautq cluster over one SimNet with a virtual clock and a
// scriptable StaticMembership per node. Nodes can be crashed (stop stepping), restarted
// (fresh QueueNode over the same data dir, new boot id), and their membership views edited
// — the SimNet analog of what the M8 chaos suite does to real processes.

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "taut/sim_net.h"

#include "queue_node.h"

namespace tautq::test {

inline taut::Endpoint ep(std::uint16_t port) {
    taut::Endpoint e{};
    e.addr_be = 1;
    e.port_be = port;
    return e;
}

struct Cluster {
    taut::SimNet net;
    std::vector<taut::Endpoint> eps;
    std::vector<std::unique_ptr<StaticMembership>> mems;
    std::vector<std::unique_ptr<QueueNode>> nodes;
    std::vector<bool> down;
    std::string base_dir;
    std::uint64_t next_boot = 0xB000;

    Cluster(std::uint64_t seed, taut::Impairments imp, int n, const std::string& name,
            NodeConfig proto = {})
        : net(seed, imp) {
        base_dir = (std::filesystem::temp_directory_path() / ("tautq-" + name)).string();
        std::filesystem::remove_all(base_dir);
        std::filesystem::create_directories(base_dir); // Wal::open only mkdirs the leaf
        for (int i = 0; i < n; ++i) {
            eps.push_back(ep(static_cast<std::uint16_t>(9000 + i)));
        }
        for (int i = 0; i < n; ++i) {
            mems.push_back(std::make_unique<StaticMembership>(eps));
            NodeConfig cfg = proto;
            cfg.self = eps[static_cast<std::size_t>(i)];
            cfg.data_dir = base_dir + "/node" + std::to_string(i);
            nodes.push_back(std::make_unique<QueueNode>(net.endpoint(cfg.self), *mems.back(), cfg,
                                                        next_boot++));
            if (!nodes.back()->open()) {
                std::abort();
            }
            down.push_back(false);
        }
    }
    ~Cluster() {
        nodes.clear();
        std::filesystem::remove_all(base_dir);
    }

    void step(std::chrono::milliseconds dt = std::chrono::milliseconds(5)) {
        net.advance(dt);
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            if (!down[i]) {
                nodes[i]->poll();
            }
        }
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            if (!down[i]) {
                nodes[i]->tick();
            }
        }
    }
    void run(int steps) {
        for (int i = 0; i < steps; ++i) {
            step();
        }
    }

    void crash(int i) {
        down[static_cast<std::size_t>(i)] = true;
    }

    // Fresh process over the same data dir: new boot id, replayed WAL.
    void restart(int i, NodeConfig proto = {}) {
        const auto idx = static_cast<std::size_t>(i);
        NodeConfig cfg = proto;
        cfg.self = eps[idx];
        cfg.data_dir = base_dir + "/node" + std::to_string(i);
        nodes[idx] =
            std::make_unique<QueueNode>(net.endpoint(cfg.self), *mems[idx], cfg, next_boot++);
        if (!nodes[idx]->open()) {
            std::abort();
        }
        down[idx] = false;
    }

    // Everyone's membership view drops/readmits node i (scripted SWIM).
    void mark_dead(int i) {
        std::vector<taut::Endpoint> alive;
        for (std::size_t k = 0; k < eps.size(); ++k) {
            if (static_cast<int>(k) != i) {
                alive.push_back(eps[k]);
            }
        }
        for (std::size_t k = 0; k < mems.size(); ++k) {
            mems[k]->set(alive);
        }
        for (std::size_t k = 0; k < nodes.size(); ++k) {
            if (static_cast<int>(k) != i && !down[k]) {
                nodes[k]->on_peer_dead(eps[static_cast<std::size_t>(i)]);
            }
        }
    }
    void mark_all_alive() {
        for (auto& m : mems) {
            m->set(eps);
        }
    }
    // Set every node's membership view to exactly these indices (scripted SWIM).
    void set_membership(std::initializer_list<int> alive_idx) {
        std::vector<taut::Endpoint> alive;
        for (int i : alive_idx) {
            alive.push_back(eps[static_cast<std::size_t>(i)]);
        }
        for (auto& m : mems) {
            m->set(alive);
        }
    }

    // The node the ring says owns this key, given full membership.
    int ring_owner_index(const std::string& key) {
        const auto owner = ring::owner_for(key, eps);
        for (std::size_t i = 0; i < eps.size(); ++i) {
            if (eps[i] == owner) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }
    // A key owned by `idx` (searches a deterministic key space).
    std::string key_owned_by(int idx, const std::string& tag) {
        for (int i = 0; i < 4096; ++i) {
            const std::string k = tag + "-" + std::to_string(i);
            if (ring_owner_index(k) == idx) {
                return k;
            }
        }
        std::abort();
    }
};

} // namespace tautq::test
