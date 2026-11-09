#include "faults.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <unistd.h>

namespace tautq::faults {

namespace {

struct State {
    std::string crash_name;
    std::uint64_t crash_nth = 0;
    std::uint64_t crash_hits = 0;
    std::uint64_t fsync_fail_at = 0;
    std::uint64_t syncs = 0;
    std::uint64_t write_fail_at = 0;
    bool short_write = false;
    std::uint64_t writes = 0;
    std::uint32_t sync_delay_us = 0;
};

State& st() {
    static State s;
    return s;
}

std::uint64_t env_u64(const char* k) {
    const char* v = std::getenv(k);
    return v == nullptr ? 0 : std::strtoull(v, nullptr, 10);
}

} // namespace

void init_from_env() {
    State& s = st();
    if (const char* cp = std::getenv("TAUTQ_CRASHPOINT"); cp != nullptr) {
        const std::string spec = cp;
        const auto colon = spec.rfind(':');
        s.crash_name = spec.substr(0, colon);
        s.crash_nth =
            colon == std::string::npos ? 1 : std::strtoull(spec.c_str() + colon + 1, nullptr, 10);
    }
    s.fsync_fail_at = env_u64("TAUTQ_FAULT_FSYNC_FAIL_AT");
    if (const auto w = env_u64("TAUTQ_FAULT_WRITE_FAIL_AT"); w != 0) {
        s.write_fail_at = w;
        s.short_write = true;
    }
    s.sync_delay_us = static_cast<std::uint32_t>(env_u64("TAUTQ_FAULT_SYNC_DELAY_US"));
    if (!s.crash_name.empty() || s.fsync_fail_at != 0 || s.write_fail_at != 0 ||
        s.sync_delay_us != 0) {
        std::fprintf(stderr,
                     "[faults] crashpoint=%s:%llu fsync_fail_at=%llu write_fail_at=%llu "
                     "sync_delay_us=%u\n",
                     s.crash_name.c_str(), static_cast<unsigned long long>(s.crash_nth),
                     static_cast<unsigned long long>(s.fsync_fail_at),
                     static_cast<unsigned long long>(s.write_fail_at), s.sync_delay_us);
    }
}

void crashpoint(const char* name) {
    State& s = st();
    if (s.crash_name.empty() || s.crash_name != name) {
        return;
    }
    if (++s.crash_hits == s.crash_nth) {
        std::fprintf(stderr, "[faults] crashpoint %s hit #%llu: SIGKILL\n", name,
                     static_cast<unsigned long long>(s.crash_hits));
        std::fflush(stderr);
        ::kill(::getpid(), SIGKILL);
        ::_exit(137); // unreachable in practice
    }
}

WalFault wal_fault(const char* op) {
    State& s = st();
    if (std::strcmp(op, "sync") == 0) {
        ++s.syncs;
        if (s.fsync_fail_at != 0 && s.syncs == s.fsync_fail_at) {
            return WalFault::FailSync;
        }
        return WalFault::None;
    }
    ++s.writes;
    if (s.write_fail_at != 0 && s.writes == s.write_fail_at) {
        return s.short_write ? WalFault::ShortWrite : WalFault::FailWrite;
    }
    return WalFault::None;
}

std::uint32_t sync_delay_us() {
    return st().sync_delay_us;
}

void reset_for_test() {
    st() = State{};
}

void set_crashpoint_for_test(const std::string& name, std::uint64_t nth) {
    st().crash_name = name;
    st().crash_nth = nth;
    st().crash_hits = 0;
}

void set_fsync_fail_at_for_test(std::uint64_t nth) {
    st().fsync_fail_at = nth;
    st().syncs = 0;
}

void set_write_fail_at_for_test(std::uint64_t nth, bool short_write) {
    st().write_fail_at = nth;
    st().short_write = short_write;
    st().writes = 0;
}

} // namespace tautq::faults
