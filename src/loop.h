#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <unordered_map>

namespace tautq {

// Minimal level-triggered epoll wrapper for the tautq binaries (Linux-only, like the rest
// of the runtime). taut's EventLoop is UDP-specific; the node process also multiplexes a
// TCP listener and its connections, so tautq owns its own loop: register fds with
// callbacks, then run_once() with a timeout that doubles as the poll()/tick() cadence for
// the protocol objects.
class Loop {
  public:
    using FdHandler = std::function<void(std::uint32_t events)>;

    Loop();
    ~Loop();
    Loop(const Loop&) = delete;
    Loop& operator=(const Loop&) = delete;

    bool ok() const {
        return epfd_ >= 0;
    }
    bool add(int fd, std::uint32_t events, FdHandler h); // events: EPOLLIN/EPOLLOUT
    bool mod(int fd, std::uint32_t events);
    void del(int fd);

    // Waits up to `timeout` and dispatches every ready fd. Returns false on fatal error.
    bool run_once(std::chrono::milliseconds timeout);

  private:
    int epfd_ = -1;
    std::unordered_map<int, FdHandler> handlers_;
};

} // namespace tautq
