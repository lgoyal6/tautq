#include "wire.h"

#include "bytes.h"

namespace tautq {

std::vector<std::byte> encode_hello(const Hello& h) {
    std::vector<std::byte> out;
    out.reserve(4 + 8 + 8);
    put_u8(out, kHelloMagic0);
    put_u8(out, kHelloMagic1);
    put_u8(out, kHelloMagic2);
    put_u8(out, h.is_ack ? 2 : 1);
    put_u64(out, h.sender_boot);
    if (h.is_ack) {
        put_u64(out, h.echo_boot);
    }
    return out;
}

bool is_hello(std::span<const std::byte> in) {
    return in.size() >= 4 && get_u8(in, 0) == kHelloMagic0 && get_u8(in, 1) == kHelloMagic1 &&
           get_u8(in, 2) == kHelloMagic2;
}

std::optional<Hello> decode_hello(std::span<const std::byte> in) {
    if (!is_hello(in)) {
        return std::nullopt;
    }
    const std::uint8_t kind = get_u8(in, 3);
    Hello h;
    if (kind == 1 && in.size() >= 12) {
        h.is_ack = false;
        h.sender_boot = get_u64(in, 4);
        return h;
    }
    if (kind == 2 && in.size() >= 20) {
        h.is_ack = true;
        h.sender_boot = get_u64(in, 4);
        h.echo_boot = get_u64(in, 12);
        return h;
    }
    return std::nullopt;
}

std::vector<std::byte> encode_rpc(MsgKind kind, Method method, std::uint64_t req_id,
                                  std::uint32_t status, std::span<const std::byte> body) {
    std::vector<std::byte> out;
    out.reserve(kRpcHeader + body.size());
    put_u8(out, static_cast<std::uint8_t>(kind));
    put_u8(out, static_cast<std::uint8_t>(method));
    put_u64(out, req_id);
    put_u32(out, status);
    put_bytes(out, body);
    return out;
}

std::optional<RpcMsg> decode_rpc(std::span<const std::byte> in) {
    if (in.size() < kRpcHeader) {
        return std::nullopt;
    }
    const std::uint8_t kind = get_u8(in, 0);
    if (kind != 1 && kind != 2) {
        return std::nullopt;
    }
    RpcMsg m;
    m.kind = static_cast<MsgKind>(kind);
    m.method = static_cast<Method>(get_u8(in, 1));
    m.req_id = get_u64(in, 2);
    m.status = get_u32(in, 10);
    m.body = in.subspan(kRpcHeader);
    return m;
}

void put_endpoint(std::vector<std::byte>& b, const taut::Endpoint& e) {
    put_u32(b, e.addr_be);
    put_u16(b, e.port_be);
}

taut::Endpoint get_endpoint(std::span<const std::byte> b, std::size_t off) {
    taut::Endpoint e;
    e.addr_be = get_u32(b, off);
    e.port_be = get_u16(b, off + 4);
    return e;
}

} // namespace tautq
