#pragma once

#include <cstdint>
#include <string>

namespace tautq::faults {

// Deterministic fault injection for real processes (crash campaign) and unit tests.
// Everything is off unless configured, and the hot-path checks are one branch each.
//
// Environment (read once by init_from_env, used by tautq-node):
//   TAUTQ_CRASHPOINT=<name>:<n>     SIGKILL this process at the n-th hit of crashpoint <name>
//   TAUTQ_FAULT_FSYNC_FAIL_AT=<n>   the n-th WAL data sync returns EIO
//   TAUTQ_FAULT_WRITE_FAIL_AT=<n>   the n-th WAL frame write fails with ENOSPC after
//                                   writing half the frame (a short write)
//   TAUTQ_FAULT_SYNC_DELAY_US=<us>  every WAL data sync sleeps this long first (slow disk)
//
// A SIGKILL leaves written-but-unsynced bytes in the kernel page cache, so a crashpoint
// models process death, not power loss. Power-loss truncation is modeled separately in
// tests by cutting each segment back to its last synced size.

void init_from_env();

// Kill the process (SIGKILL) if `name` is the configured crashpoint and this is its n-th hit.
void crashpoint(const char* name);

enum class WalFault { None, FailWrite, ShortWrite, FailSync };

// Consulted by the WAL before each frame write / data sync. `op` is "write" or "sync".
WalFault wal_fault(const char* op);
std::uint32_t sync_delay_us();

// Test controls (process-global; tests reset them).
void reset_for_test();
void set_crashpoint_for_test(const std::string& name, std::uint64_t nth);
void set_fsync_fail_at_for_test(std::uint64_t nth);
void set_write_fail_at_for_test(std::uint64_t nth, bool short_write);

} // namespace tautq::faults
