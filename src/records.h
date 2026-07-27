#pragma once

#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

#include "job.h"

namespace tautq {

// Log record bodies (DESIGN-protocol §3/§4). One record per state transition; SUBMIT and
// REPLICATE carry the whole job description (replicas learn everything from their copy),
// the rest reference a JobId + the fencing fields. The same encoding travels inside
// Replicate RPCs, so a replica appends exactly the bytes the owner committed.
//
//   u8 type | u64 lsn | payload
//
// The lsn is the APPENDING node's local sequence — it exists for debugging and the chaos
// verifier; replay order is positional.

enum class RecType : std::uint8_t {
    Submit = 1,     // owner accepted a job: full Job (state fields at initial values)
    Replicate = 2,  // replica stored a copy: full Job + the epoch it was sent under
    Lease = 3,      // job_id, epoch, lease_seq, worker_id
    Done = 4,       // job_id, epoch, lease_seq
    Expire = 5,     // job_id, epoch, lease_seq (lease returned to Ready)
    DeadLetter = 6, // job_id, epoch (attempts exhausted)
    Takeover = 7,   // job_id, new_epoch (this node is now owner)
};

struct SubmitRec {
    Job job;
};
struct ReplicateRec {
    Job job;
};
struct LeaseRec {
    JobId id;
    std::uint32_t epoch = 0;
    std::uint32_t lease_seq = 0;
    std::uint64_t worker = 0;
};
struct DoneRec {
    JobId id;
    std::uint32_t epoch = 0;
    std::uint32_t lease_seq = 0;
};
struct ExpireRec {
    JobId id;
    std::uint32_t epoch = 0;
    std::uint32_t lease_seq = 0;
};
struct DeadLetterRec {
    JobId id;
    std::uint32_t epoch = 0;
};
struct TakeoverRec {
    JobId id;
    std::uint32_t new_epoch = 0;
};

using Record =
    std::variant<SubmitRec, ReplicateRec, LeaseRec, DoneRec, ExpireRec, DeadLetterRec, TakeoverRec>;

std::vector<std::byte> encode_record(const Record& r, std::uint64_t lsn);
// Decodes a record body (as handed back by Wal::replay). nullopt on malformed input.
std::optional<Record> decode_record(taut::ByteSpan body, std::uint64_t* lsn_out = nullptr);

} // namespace tautq
