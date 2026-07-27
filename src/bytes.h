#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace tautq {

// Little-endian byte helpers shared by the wire codec and the log. Same shift-based style
// as taut's codec (no memcpy-through-cast, no alignment assumptions).

inline void put_u8(std::vector<std::byte>& b, std::uint8_t v) {
    b.push_back(std::byte{v});
}
inline void put_u16(std::vector<std::byte>& b, std::uint16_t v) {
    b.push_back(std::byte{static_cast<std::uint8_t>(v & 0xFFu)});
    b.push_back(std::byte{static_cast<std::uint8_t>((v >> 8) & 0xFFu)});
}
inline void put_u32(std::vector<std::byte>& b, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        b.push_back(std::byte{static_cast<std::uint8_t>((v >> (8 * i)) & 0xFFu)});
    }
}
inline void put_u64(std::vector<std::byte>& b, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        b.push_back(std::byte{static_cast<std::uint8_t>((v >> (8 * i)) & 0xFFu)});
    }
}
inline void put_bytes(std::vector<std::byte>& b, std::span<const std::byte> v) {
    b.insert(b.end(), v.begin(), v.end());
}

inline std::uint8_t get_u8(std::span<const std::byte> b, std::size_t off) {
    return std::to_integer<std::uint8_t>(b[off]);
}
inline std::uint16_t get_u16(std::span<const std::byte> b, std::size_t off) {
    return static_cast<std::uint16_t>(std::to_integer<unsigned>(b[off]) |
                                      (std::to_integer<unsigned>(b[off + 1]) << 8));
}
inline std::uint32_t get_u32(std::span<const std::byte> b, std::size_t off) {
    return static_cast<std::uint32_t>(std::to_integer<unsigned>(b[off])) |
           (static_cast<std::uint32_t>(std::to_integer<unsigned>(b[off + 1])) << 8) |
           (static_cast<std::uint32_t>(std::to_integer<unsigned>(b[off + 2])) << 16) |
           (static_cast<std::uint32_t>(std::to_integer<unsigned>(b[off + 3])) << 24);
}
inline std::uint64_t get_u64(std::span<const std::byte> b, std::size_t off) {
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i) {
        v = (v << 8) | std::to_integer<std::uint64_t>(b[off + static_cast<std::size_t>(i)]);
    }
    return v;
}

} // namespace tautq
