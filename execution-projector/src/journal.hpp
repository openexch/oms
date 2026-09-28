// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace oe {
inline constexpr std::size_t MAX_JOURNAL_FRAME_LENGTH = 124; // schema 1/v11 template 28 (8 + 116)

struct Event {
    std::int64_t position{}; // End position of the complete Aeron message.
    std::vector<std::uint8_t> bytes;
};
struct Journal {
    std::uint16_t type{};
    std::int64_t seq{}, trade{}, taker{}, maker{}, takerUser{}, makerUser{};
    std::int64_t price{}, quantity{}, timestamp{}, takerOms{}, makerOms{};
    std::int32_t market{};
    std::uint8_t sideOrStatus{};
    std::int64_t commandHigh{}, commandLow{}, oldOrder{}, budget{}, appliedPosition{};
    std::int32_t kind{}, orderType{}, orderSide{}, status{}, reason{}, result{};
    bool oldCancelled{};
};
// Fixed-length settlement schema 3/v1 and command outcomes schema 1/v11.
// Fail closed on an unknown schema rather than
// checkpointing an event that this binary cannot project.
Journal decode(std::span<const std::uint8_t> bytes);
std::string hex(std::span<const std::uint8_t> bytes);
}
