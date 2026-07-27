#include "records.h"

#include <gtest/gtest.h>

namespace {

taut::Endpoint ep(std::uint16_t port) {
    taut::Endpoint e{};
    e.addr_be = 1;
    e.port_be = port;
    return e;
}

tautq::Job sample_job() {
    tautq::Job j;
    j.id = {0x00010001ull << 16 | 9000, (0xB001ull << 32) | 7};
    j.idem_key = "order-1234";
    j.url = "http://10.9.0.99:8081/hook";
    j.body = {std::byte{'h'}, std::byte{'i'}};
    j.visibility_ms = 15000;
    j.max_attempts = 3;
    j.replicas = {ep(9000), ep(9001), ep(9002)};
    j.state = tautq::JobState::Ready;
    j.epoch = 1;
    j.lease_seq = 0;
    j.attempts = 0;
    return j;
}

void expect_job_eq(const tautq::Job& a, const tautq::Job& b) {
    EXPECT_EQ(a.id, b.id);
    EXPECT_EQ(a.idem_key, b.idem_key);
    EXPECT_EQ(a.url, b.url);
    EXPECT_EQ(a.body, b.body);
    EXPECT_EQ(a.visibility_ms, b.visibility_ms);
    EXPECT_EQ(a.max_attempts, b.max_attempts);
    EXPECT_EQ(a.replicas, b.replicas);
    EXPECT_EQ(a.state, b.state);
    EXPECT_EQ(a.epoch, b.epoch);
    EXPECT_EQ(a.lease_seq, b.lease_seq);
    EXPECT_EQ(a.attempts, b.attempts);
}

TEST(Records, JobIdHexRoundTrip) {
    const tautq::JobId id{0xDEADBEEF12345678ull, 0x0102030405060708ull};
    const auto s = tautq::to_hex(id);
    EXPECT_EQ(s.size(), 32u);
    tautq::JobId back;
    ASSERT_TRUE(tautq::from_hex(s, back));
    EXPECT_EQ(back, id);
    EXPECT_FALSE(tautq::from_hex("nope", back));
    EXPECT_FALSE(tautq::from_hex(std::string(32, 'g'), back));
}

TEST(Records, JobRoundTrip) {
    const auto j = sample_job();
    std::vector<std::byte> buf;
    tautq::put_job(buf, j);
    tautq::Job back;
    ASSERT_GT(tautq::get_job(buf, 0, back), 0u);
    expect_job_eq(back, j);
}

TEST(Records, JobRejectsTruncation) {
    const auto j = sample_job();
    std::vector<std::byte> buf;
    tautq::put_job(buf, j);
    tautq::Job back;
    for (std::size_t cut = 0; cut < buf.size(); cut += 7) {
        EXPECT_EQ(tautq::get_job(std::span<const std::byte>(buf.data(), cut), 0, back), 0u)
            << "prefix of " << cut << " bytes must be rejected";
    }
}

TEST(Records, AllRecordTypesRoundTrip) {
    const tautq::JobId id{1, 2};
    const std::vector<tautq::Record> records = {
        tautq::SubmitRec{sample_job()},    tautq::ReplicateRec{sample_job()},
        tautq::LeaseRec{id, 3, 4, 0xFEED}, tautq::DoneRec{id, 3, 4},
        tautq::ExpireRec{id, 3, 4},        tautq::DeadLetterRec{id, 3},
        tautq::TakeoverRec{id, 7},
    };
    std::uint64_t lsn = 100;
    for (const auto& r : records) {
        const auto body = tautq::encode_record(r, lsn);
        std::uint64_t got_lsn = 0;
        const auto back = tautq::decode_record(body, &got_lsn);
        ASSERT_TRUE(back.has_value());
        EXPECT_EQ(got_lsn, lsn);
        EXPECT_EQ(back->index(), r.index());
        ++lsn;
    }
    // Spot-check one payload deeply.
    const auto lease =
        tautq::decode_record(tautq::encode_record(tautq::LeaseRec{id, 9, 8, 0xAB}, 1));
    const auto* l = std::get_if<tautq::LeaseRec>(&*lease);
    ASSERT_NE(l, nullptr);
    EXPECT_EQ(l->epoch, 9u);
    EXPECT_EQ(l->lease_seq, 8u);
    EXPECT_EQ(l->worker, 0xABu);
}

TEST(Records, DecodeRejectsGarbage) {
    std::vector<std::byte> junk(5, std::byte{0x77});
    EXPECT_FALSE(tautq::decode_record(junk).has_value());
    std::vector<std::byte> unknown_type(64, std::byte{0});
    unknown_type[0] = std::byte{99};
    EXPECT_FALSE(tautq::decode_record(unknown_type).has_value());
}

} // namespace
