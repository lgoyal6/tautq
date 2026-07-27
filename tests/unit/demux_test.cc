#include "demux.h"

#include <array>
#include <chrono>
#include <vector>

#include <gtest/gtest.h>

#include "taut/codec.h"
#include "taut/sim_net.h"

using namespace std::chrono_literals;

namespace {

taut::Endpoint ep(std::uint16_t port) {
    taut::Endpoint e{};
    e.addr_be = 1;
    e.port_be = port;
    return e;
}

std::vector<std::byte> taut_datagram(std::uint32_t seq) {
    taut::Packet p{};
    p.type = taut::PacketType::Data;
    p.cls = taut::Class::ReliableUnordered;
    p.seq = seq;
    const std::array<std::byte, 3> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    p.payload = payload;
    std::array<std::byte, taut::kMaxDatagram> buf{};
    const std::size_t n = taut::encode(p, buf);
    return {buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(n)};
}

TEST(Demux, SplitsHellosStrangersAndPeerTraffic) {
    taut::SimNet net(1, taut::Impairments{});
    const auto a = ep(9000);
    const auto b = ep(9001);
    const auto c = ep(9002);

    tautq::Demux demux(net.endpoint(a));
    std::vector<std::pair<taut::Endpoint, tautq::Hello>> hellos;
    std::vector<taut::Endpoint> strangers;
    demux.on_hello(
        [&](const taut::Endpoint& from, const tautq::Hello& h) { hellos.emplace_back(from, h); });
    demux.on_stranger([&](const taut::Endpoint& from) { strangers.push_back(from); });

    // B sends a HELLO; C sends taut traffic without any handshake.
    tautq::Hello h;
    h.sender_boot = 123;
    net.endpoint(b).send(a, tautq::encode_hello(h));
    net.endpoint(c).send(a, taut_datagram(0));
    net.advance(1ms);
    demux.pump();

    ASSERT_EQ(hellos.size(), 1u);
    EXPECT_EQ(hellos[0].first, b);
    EXPECT_EQ(hellos[0].second.sender_boot, 123u);
    ASSERT_EQ(strangers.size(), 1u);
    EXPECT_EQ(strangers[0], c);

    // Once C has a view, its traffic lands in that view's inbox — and only there.
    tautq::PeerView& vc = demux.view(c);
    tautq::PeerView& vb = demux.view(b);
    net.endpoint(c).send(a, taut_datagram(1));
    net.advance(1ms);
    demux.pump();

    std::array<std::byte, taut::kMaxDatagram> buf{};
    ASSERT_FALSE(vb.recv(buf).has_value());
    const auto r = vc.recv(buf);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->from, c);
    taut::Packet p{};
    ASSERT_EQ(taut::decode(std::span<const std::byte>(buf.data(), r->size), p),
              taut::DecodeError::Ok);
    EXPECT_EQ(p.seq, 1u);
    EXPECT_FALSE(vc.recv(buf).has_value());
}

TEST(Demux, DropDiscardsQueuedTraffic) {
    taut::SimNet net(2, taut::Impairments{});
    const auto a = ep(9000);
    const auto b = ep(9001);
    tautq::Demux demux(net.endpoint(a));
    demux.view(b);
    net.endpoint(b).send(a, taut_datagram(0));
    net.advance(1ms);
    demux.pump();
    demux.drop(b);

    // A fresh view starts empty; the pre-drop datagram is gone.
    std::array<std::byte, taut::kMaxDatagram> buf{};
    EXPECT_FALSE(demux.view(b).recv(buf).has_value());
}

} // namespace
