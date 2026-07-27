#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "taut/transport.h" // Endpoint
#include "taut/types.h"     // ByteSpan

namespace tautq {

// ---- HELLO datagrams (below taut, DESIGN-protocol §6) --------------------------------------
//
// taut sessions have no connection handshake, so a restarted peer's fresh sequence space
// looks like replay to anyone holding old session state. tautq resets sessions above the
// library: every process draws a random boot_id, and peers exchange it in raw HELLO
// datagrams that deliberately do NOT start with taut's 0x7A75 magic — the demux splits
// traffic on the first two bytes, so HELLOs work even when no session exists yet.
//
//   "TQ" 'H' kind(u8: 1=hello, 2=ack) | u64 sender_boot | u64 echo_boot (ack only)
//
// A HELLO advertises the sender's boot; the ACK echoes the boot it is acknowledging, so a
// stale ACK (from before our restart) cannot complete the new handshake.

inline constexpr std::uint8_t kHelloMagic0 = 'T';
inline constexpr std::uint8_t kHelloMagic1 = 'Q';
inline constexpr std::uint8_t kHelloMagic2 = 'H';

struct Hello {
    bool is_ack = false;
    std::uint64_t sender_boot = 0;
    std::uint64_t echo_boot = 0; // ack only: the boot_id being acknowledged
};

std::vector<std::byte> encode_hello(const Hello& h);
std::optional<Hello> decode_hello(std::span<const std::byte> in);

// True if the datagram is a tautq HELLO (vs. a taut packet to feed the session).
bool is_hello(std::span<const std::byte> in);

// ---- RPC envelope (inside taut class-1 payloads) --------------------------------------------
//
//   u8 kind (1=request, 2=response) | u8 method | u64 req_id | u32 status | body...
//
// status is 0 in requests; in responses it is the handler's application status (0 = ok).
// Transport-level failures (timeout, peer down) never appear on the wire — the RPC layer
// synthesizes them locally (see rpc.h Status).

enum class MsgKind : std::uint8_t { Request = 1, Response = 2 };

// Method space for the whole protocol (DESIGN-protocol §3/§6); later modules claim theirs.
enum class Method : std::uint8_t {
    Ping = 1,         // liveness/testing echo
    FwdSubmit = 2,    // gateway -> owner: submit on behalf of a client
    Replicate = 3,    // owner -> replica: append a replicated record
    Claim = 4,        // successor -> replica: majority takeover
    Resync = 5,       // restarted node -> replica set: reconcile per-job state
    FwdAck = 6,       // any node -> owner: forward a worker's completion
    FwdStatus = 7,    // any node -> owner: job status query
    Apply = 8,        // owner -> replica: append one transition record (Lease/Done/Expire/...)
    DrainHandoff = 9, // draining owner -> successor: please claim this job from me
};

inline constexpr std::size_t kRpcHeader = 14; // kind + method + req_id + status

struct RpcMsg {
    MsgKind kind = MsgKind::Request;
    Method method = Method::Ping;
    std::uint64_t req_id = 0;
    std::uint32_t status = 0;
    taut::ByteSpan body; // view into the decoded buffer
};

std::vector<std::byte> encode_rpc(MsgKind kind, Method method, std::uint64_t req_id,
                                  std::uint32_t status, std::span<const std::byte> body);
std::optional<RpcMsg> decode_rpc(std::span<const std::byte> in);

// Endpoint (de)serialization used across RPC bodies and the log: u32 addr_be | u16 port_be.
void put_endpoint(std::vector<std::byte>& b, const taut::Endpoint& e);
taut::Endpoint get_endpoint(std::span<const std::byte> b, std::size_t off);
inline constexpr std::size_t kEndpointSize = 6;

// Stable identity key for an endpoint (same shape taut's swim.cc uses internally).
inline std::uint64_t ekey(const taut::Endpoint& e) {
    return (static_cast<std::uint64_t>(e.addr_be) << 16) | e.port_be;
}

} // namespace tautq
