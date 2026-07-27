#include "records.h"

#include <cstdio>

#include "bytes.h"

namespace tautq {

std::string to_hex(const JobId& id) {
    char buf[33];
    std::snprintf(buf, sizeof buf, "%016llx%016llx", static_cast<unsigned long long>(id.origin),
                  static_cast<unsigned long long>(id.nonce));
    return buf;
}

bool from_hex(const std::string& s, JobId& id) {
    if (s.size() != 32) {
        return false;
    }
    std::uint64_t v[2] = {0, 0};
    for (int half = 0; half < 2; ++half) {
        for (int i = 0; i < 16; ++i) {
            const char c = s[static_cast<std::size_t>(half * 16 + i)];
            std::uint64_t d = 0;
            if (c >= '0' && c <= '9') {
                d = static_cast<std::uint64_t>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                d = static_cast<std::uint64_t>(c - 'a' + 10);
            } else {
                return false;
            }
            v[half] = (v[half] << 4) | d;
        }
    }
    id.origin = v[0];
    id.nonce = v[1];
    return true;
}

bool job_advances(const Job& local, const Job& remote) {
    if (remote.epoch != local.epoch) {
        return remote.epoch > local.epoch;
    }
    if ((remote.state == JobState::Done) != (local.state == JobState::Done)) {
        return remote.state == JobState::Done;
    }
    if ((remote.state == JobState::DeadLetter) != (local.state == JobState::DeadLetter)) {
        return remote.state == JobState::DeadLetter;
    }
    if (remote.lease_seq != local.lease_seq) {
        return remote.lease_seq > local.lease_seq;
    }
    return remote.state == JobState::Leased && local.state == JobState::Ready;
}

void put_job_id(std::vector<std::byte>& out, const JobId& id) {
    put_u64(out, id.origin);
    put_u64(out, id.nonce);
}

JobId get_job_id(taut::ByteSpan in, std::size_t off) {
    JobId id;
    id.origin = get_u64(in, off);
    id.nonce = get_u64(in, off + 8);
    return id;
}

namespace {

void put_str(std::vector<std::byte>& out, const std::string& s) {
    put_u16(out, static_cast<std::uint16_t>(s.size()));
    for (char c : s) {
        out.push_back(std::byte{static_cast<unsigned char>(c)});
    }
}

// Reads a u16-prefixed string; advances off. Returns false past-the-end.
bool get_str(taut::ByteSpan in, std::size_t& off, std::string& s, std::size_t max) {
    if (off + 2 > in.size()) {
        return false;
    }
    const std::size_t n = get_u16(in, off);
    off += 2;
    if (n > max || off + n > in.size()) {
        return false;
    }
    s.resize(n);
    for (std::size_t i = 0; i < n; ++i) {
        s[i] = static_cast<char>(std::to_integer<unsigned char>(in[off + i]));
    }
    off += n;
    return true;
}

} // namespace

void put_job(std::vector<std::byte>& out, const Job& j) {
    put_job_id(out, j.id);
    put_str(out, j.idem_key);
    put_str(out, j.url);
    put_u16(out, static_cast<std::uint16_t>(j.body.size()));
    put_bytes(out, j.body);
    put_u32(out, j.visibility_ms);
    put_u32(out, j.max_attempts);
    for (const auto& r : j.replicas) {
        put_endpoint(out, r);
    }
    put_u8(out, static_cast<std::uint8_t>(j.state));
    put_endpoint(out, j.owner);
    put_u32(out, j.epoch);
    put_u32(out, j.lease_seq);
    put_u32(out, j.attempts);
}

std::size_t get_job(taut::ByteSpan in, std::size_t off, Job& j) {
    const std::size_t start = off;
    if (off + kJobIdSize > in.size()) {
        return 0;
    }
    j.id = get_job_id(in, off);
    off += kJobIdSize;
    if (!get_str(in, off, j.idem_key, kMaxIdemKey) || !get_str(in, off, j.url, kMaxUrl)) {
        return 0;
    }
    if (off + 2 > in.size()) {
        return 0;
    }
    const std::size_t blen = get_u16(in, off);
    off += 2;
    if (blen > kMaxJobBody || off + blen > in.size()) {
        return 0;
    }
    j.body.assign(in.begin() + static_cast<std::ptrdiff_t>(off),
                  in.begin() + static_cast<std::ptrdiff_t>(off + blen));
    off += blen;
    if (off + 4 + 4 + 4 * kEndpointSize + 1 + 4 + 4 + 4 > in.size()) {
        return 0;
    }
    j.visibility_ms = get_u32(in, off);
    off += 4;
    j.max_attempts = get_u32(in, off);
    off += 4;
    for (auto& r : j.replicas) {
        r = get_endpoint(in, off);
        off += kEndpointSize;
    }
    j.state = static_cast<JobState>(get_u8(in, off));
    off += 1;
    j.owner = get_endpoint(in, off);
    off += kEndpointSize;
    j.epoch = get_u32(in, off);
    off += 4;
    j.lease_seq = get_u32(in, off);
    off += 4;
    j.attempts = get_u32(in, off);
    off += 4;
    return off - start;
}

namespace {

void put_fence(std::vector<std::byte>& out, const JobId& id, std::uint32_t epoch,
               std::uint32_t lease_seq) {
    put_job_id(out, id);
    put_u32(out, epoch);
    put_u32(out, lease_seq);
}

} // namespace

std::vector<std::byte> encode_record(const Record& r, std::uint64_t lsn) {
    std::vector<std::byte> out;
    const auto tag = [&](RecType t) {
        put_u8(out, static_cast<std::uint8_t>(t));
        put_u64(out, lsn);
    };
    if (const auto* s = std::get_if<SubmitRec>(&r)) {
        tag(RecType::Submit);
        put_job(out, s->job);
    } else if (const auto* rep = std::get_if<ReplicateRec>(&r)) {
        tag(RecType::Replicate);
        put_job(out, rep->job);
    } else if (const auto* l = std::get_if<LeaseRec>(&r)) {
        tag(RecType::Lease);
        put_fence(out, l->id, l->epoch, l->lease_seq);
        put_u64(out, l->worker);
    } else if (const auto* d = std::get_if<DoneRec>(&r)) {
        tag(RecType::Done);
        put_fence(out, d->id, d->epoch, d->lease_seq);
    } else if (const auto* e = std::get_if<ExpireRec>(&r)) {
        tag(RecType::Expire);
        put_fence(out, e->id, e->epoch, e->lease_seq);
    } else if (const auto* dl = std::get_if<DeadLetterRec>(&r)) {
        tag(RecType::DeadLetter);
        put_job_id(out, dl->id);
        put_u32(out, dl->epoch);
    } else if (const auto* t = std::get_if<TakeoverRec>(&r)) {
        tag(RecType::Takeover);
        put_job_id(out, t->id);
        put_u32(out, t->new_epoch);
        put_endpoint(out, t->new_owner);
    }
    return out;
}

std::optional<Record> decode_record(taut::ByteSpan body, std::uint64_t* lsn_out) {
    if (body.size() < 9) {
        return std::nullopt;
    }
    const auto type = static_cast<RecType>(get_u8(body, 0));
    if (lsn_out != nullptr) {
        *lsn_out = get_u64(body, 1);
    }
    std::size_t off = 9;

    switch (type) {
    case RecType::Submit:
    case RecType::Replicate: {
        Job j;
        if (get_job(body, off, j) == 0) {
            return std::nullopt;
        }
        if (type == RecType::Submit) {
            return Record{SubmitRec{std::move(j)}};
        }
        return Record{ReplicateRec{std::move(j)}};
    }
    case RecType::Lease: {
        if (off + kJobIdSize + 4 + 4 + 8 > body.size()) {
            return std::nullopt;
        }
        LeaseRec l;
        l.id = get_job_id(body, off);
        l.epoch = get_u32(body, off + kJobIdSize);
        l.lease_seq = get_u32(body, off + kJobIdSize + 4);
        l.worker = get_u64(body, off + kJobIdSize + 8);
        return Record{l};
    }
    case RecType::Done:
    case RecType::Expire: {
        if (off + kJobIdSize + 8 > body.size()) {
            return std::nullopt;
        }
        const JobId id = get_job_id(body, off);
        const std::uint32_t epoch = get_u32(body, off + kJobIdSize);
        const std::uint32_t seq = get_u32(body, off + kJobIdSize + 4);
        if (type == RecType::Done) {
            return Record{DoneRec{id, epoch, seq}};
        }
        return Record{ExpireRec{id, epoch, seq}};
    }
    case RecType::DeadLetter: {
        if (off + kJobIdSize + 4 > body.size()) {
            return std::nullopt;
        }
        return Record{DeadLetterRec{get_job_id(body, off), get_u32(body, off + kJobIdSize)}};
    }
    case RecType::Takeover: {
        if (off + kJobIdSize + 4 + kEndpointSize > body.size()) {
            return std::nullopt;
        }
        return Record{TakeoverRec{get_job_id(body, off), get_u32(body, off + kJobIdSize),
                                  get_endpoint(body, off + kJobIdSize + 4)}};
    }
    }
    return std::nullopt;
}

} // namespace tautq
