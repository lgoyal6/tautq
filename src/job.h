#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "taut/transport.h"

#include "wire.h"

namespace tautq {

// Job lifecycle (DESIGN-protocol §1): Ready -> Leased -> Done; lease expiry returns
// Leased -> Ready; attempts exhaustion parks the job in DeadLetter (terminal, queryable).
enum class JobState : std::uint8_t {
    Ready = 1,
    Leased = 2,
    Done = 3,
    DeadLetter = 4,
};

// Globally unique with zero coordination: the creating owner's endpoint key + a nonce that
// mixes the creator's boot_id (top 32 bits) with an owner-local counter — two incarnations
// of the same node can never mint the same id.
struct JobId {
    std::uint64_t origin = 0; // ekey() of the owner endpoint at creation
    std::uint64_t nonce = 0;  // (boot_id low 32) << 32 | local seq

    bool operator==(const JobId&) const = default;
};

struct JobIdHash {
    std::size_t operator()(const JobId& id) const {
        return std::hash<std::uint64_t>{}(id.origin ^ (id.nonce * 0x9E3779B97F4A7C15ull));
    }
};

std::string to_hex(const JobId& id);            // 32 hex chars, HTTP-facing
bool from_hex(const std::string& s, JobId& id); // strict parse

// A job's immutable description (set at submit) plus its replicated dynamic state. Lease
// deadlines are deliberately NOT here and never hit the log: they are in-memory only, and a
// restarted/taking-over node re-arms them conservatively (now + visibility) — so the log
// needs no wall clock and replay is time-independent.
struct Job {
    JobId id;
    std::string idem_key;                     // client idempotency key
    std::string url;                          // webhook destination
    std::vector<std::byte> body;              // opaque payload, delivered as the POST body
    std::uint32_t visibility_ms = 30000;      // lease visibility timeout
    std::uint32_t max_attempts = 5;           // then DeadLetter
    std::array<taut::Endpoint, 3> replicas{}; // [0] = owner at creation; successor order

    JobState state = JobState::Ready;
    taut::Endpoint owner{};      // current owner (replicas[0] at birth; moves on Takeover)
    std::uint32_t epoch = 1;     // owner epoch e (fencing)
    std::uint32_t lease_seq = 0; // increments per grant; (epoch, lease_seq) = lease token
    std::uint32_t attempts = 0;  // == lease grants so far; DeadLetter at max_attempts
};

// Serialized-job budget: a full job description must fit one REPLICATE RPC (kMaxRpcBody)
// with room for the record framing around it.
inline constexpr std::size_t kMaxIdemKey = 64;
inline constexpr std::size_t kMaxUrl = 256;
inline constexpr std::size_t kMaxJobBody = 640;

void put_job(std::vector<std::byte>& out, const Job& j);
// Returns bytes consumed, or 0 on malformed input.
std::size_t get_job(taut::ByteSpan in, std::size_t off, Job& j);

void put_job_id(std::vector<std::byte>& out, const JobId& id);
JobId get_job_id(taut::ByteSpan in, std::size_t off);
inline constexpr std::size_t kJobIdSize = 16;

// The one ordering every full-copy merge path (Replicate, RESYNC, claim info) obeys: a
// remote copy is adopted only if it STRICTLY advances the local one. Precedence: higher
// epoch; then Done (absolute) > DeadLetter > higher lease_seq > Leased-over-Ready. Anything
// else keeps local state — so a replica that never saw a committed lease can't erase it.
bool job_advances(const Job& local, const Job& remote);

} // namespace tautq
