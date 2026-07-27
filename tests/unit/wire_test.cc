#include "wire.h"

#include <cstddef>
#include <vector>

#include <gtest/gtest.h>

namespace {

std::vector<std::byte> bytes(std::initializer_list<int> v) {
    std::vector<std::byte> out;
    for (int x : v) {
        out.push_back(std::byte{static_cast<unsigned char>(x)});
    }
    return out;
}

TEST(Wire, HelloRoundTrip) {
    tautq::Hello h;
    h.is_ack = false;
    h.sender_boot = 0x1122334455667788ull;
    const auto d = tautq::encode_hello(h);
    ASSERT_TRUE(tautq::is_hello(d));
    const auto back = tautq::decode_hello(d);
    ASSERT_TRUE(back.has_value());
    EXPECT_FALSE(back->is_ack);
    EXPECT_EQ(back->sender_boot, h.sender_boot);
}

TEST(Wire, HelloAckRoundTripCarriesEcho) {
    tautq::Hello h;
    h.is_ack = true;
    h.sender_boot = 42;
    h.echo_boot = 77;
    const auto d = tautq::encode_hello(h);
    const auto back = tautq::decode_hello(d);
    ASSERT_TRUE(back.has_value());
    EXPECT_TRUE(back->is_ack);
    EXPECT_EQ(back->sender_boot, 42u);
    EXPECT_EQ(back->echo_boot, 77u);
}

TEST(Wire, HelloRejectsTruncationAndForeignMagic) {
    tautq::Hello h;
    h.is_ack = false;
    h.sender_boot = 7;
    auto d = tautq::encode_hello(h);
    d.resize(d.size() - 1);
    EXPECT_FALSE(tautq::decode_hello(d).has_value());

    // A taut packet (0x7A 0x75 magic) must never look like a HELLO.
    const auto taut_like = bytes({0x7A, 0x75, 0x11, 0x00, 0x02});
    EXPECT_FALSE(tautq::is_hello(taut_like));
}

TEST(Wire, RpcRoundTrip) {
    const auto body = bytes({1, 2, 3, 4, 5});
    const auto frame =
        tautq::encode_rpc(tautq::MsgKind::Request, tautq::Method::Replicate, 99, 0, body);
    const auto m = tautq::decode_rpc(frame);
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->kind, tautq::MsgKind::Request);
    EXPECT_EQ(m->method, tautq::Method::Replicate);
    EXPECT_EQ(m->req_id, 99u);
    EXPECT_EQ(m->status, 0u);
    ASSERT_EQ(m->body.size(), body.size());
    EXPECT_TRUE(std::equal(m->body.begin(), m->body.end(), body.begin()));
}

TEST(Wire, RpcRejectsShortAndBadKind) {
    EXPECT_FALSE(tautq::decode_rpc(bytes({1, 2, 3})).has_value());
    auto frame = tautq::encode_rpc(tautq::MsgKind::Response, tautq::Method::Ping, 1, 0, {});
    frame[0] = std::byte{9}; // invalid kind
    EXPECT_FALSE(tautq::decode_rpc(frame).has_value());
}

TEST(Wire, EndpointRoundTrip) {
    taut::Endpoint e;
    e.addr_be = 0xAABBCCDD;
    e.port_be = 0x1234;
    std::vector<std::byte> b;
    tautq::put_endpoint(b, e);
    ASSERT_EQ(b.size(), tautq::kEndpointSize);
    EXPECT_EQ(tautq::get_endpoint(b, 0), e);
}

} // namespace
