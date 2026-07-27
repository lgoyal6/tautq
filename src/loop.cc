#include "loop.h"

#include <array>
#include <cerrno>

#include <sys/epoll.h>
#include <unistd.h>

namespace tautq {

Loop::Loop() : epfd_(::epoll_create1(EPOLL_CLOEXEC)) {}

Loop::~Loop() {
    if (epfd_ >= 0) {
        ::close(epfd_);
    }
}

bool Loop::add(int fd, std::uint32_t events, FdHandler h) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        return false;
    }
    handlers_[fd] = std::move(h);
    return true;
}

bool Loop::mod(int fd, std::uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) == 0;
}

void Loop::del(int fd) {
    ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    handlers_.erase(fd);
}

bool Loop::run_once(std::chrono::milliseconds timeout) {
    std::array<epoll_event, 64> events{};
    const int n = ::epoll_wait(epfd_, events.data(), static_cast<int>(events.size()),
                               static_cast<int>(timeout.count()));
    if (n < 0) {
        return errno == EINTR;
    }
    for (int i = 0; i < n; ++i) {
        const int fd = events[static_cast<std::size_t>(i)].data.fd;
        auto it = handlers_.find(fd);
        if (it != handlers_.end()) {
            // Copy: the handler may del() its own fd (connection close).
            FdHandler h = it->second;
            h(events[static_cast<std::size_t>(i)].events);
        }
    }
    return true;
}

} // namespace tautq
