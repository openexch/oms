// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace oe {
// ME journal schema 4 / version 1 (RC2 S1 contract section 2). The largest legal frame is
// CommandRelease with 48 entries: 8 header + 44 root + 4 group header + 48 * 22.
inline constexpr std::uint16_t JOURNAL_SCHEMA_ID = 4;
inline constexpr std::uint16_t JOURNAL_SCHEMA_VERSION = 1;
inline constexpr std::size_t MAX_RELEASE_ENTRIES = 48;
inline constexpr std::size_t MAX_JOURNAL_FRAME_LENGTH = 1112;
static_assert(MAX_JOURNAL_FRAME_LENGTH == 8 + 44 + 4 + MAX_RELEASE_ENTRIES * 22);

// Record template ids on the journal stream.
inline constexpr std::uint16_t RECORD_TRADE = 1, RECORD_TERMINAL = 2, RECORD_OUTCOME = 3,
        RECORD_RELEASE = 4, RECORD_CONFIG = 5;

// Byte range of a CommandOutcome frame that identifies the outcome of one command id: the 20
// template-28 fields (commandIdHigh .. result), which is what the behaviour-1 ME compares for a
// DUPLICATE plus the replayed outcome. Excluded: the header and journalSeq/meEpoch/egressSeq
// (delivery); the intent echo timeInForce/expectedOldRemaining, which a behaviour-1 DUPLICATE
// carries from the retry frame (Engine.java:74-86); and the ledgerSize/ledgerCapacity/
// behaviorVersion sample the ME takes when it journals the record.
inline constexpr std::size_t OUTCOME_CANONICAL_BEGIN = 32, OUTCOME_CANONICAL_END = 140;

struct Event {
    std::int64_t position{}; // End position of the complete Aeron message.
    std::vector<std::uint8_t> bytes;
};
struct Release {
    std::int64_t commandHigh{}, idLowFloor{};
    std::uint8_t scope{}, result{};
    std::int32_t removed{};
};
struct Journal {
    std::uint16_t type{}; // Record template id 1..5.
    std::int64_t journalSeq{}, meEpoch{}, egressSeq{};
    // Trade (taker = taker order) and terminal (taker = order) identity.
    std::int64_t trade{}, taker{}, maker{}, takerUser{}, makerUser{};
    std::int64_t price{}, quantity{}, timestamp{}, takerOms{}, makerOms{};
    std::int32_t market{};
    std::uint8_t sideOrStatus{}, rejectReason{};
    // CommandOutcome.
    std::int64_t commandHigh{}, commandLow{}, oldOrder{}, budget{}, appliedPosition{}, expectedOldRemaining{};
    std::int32_t kind{}, orderType{}, orderSide{}, status{}, reason{}, result{}, timeInForce{};
    std::int32_t ledgerSize{};
    std::int64_t ledgerCapacity{}; // int32 on records 3/4, uint32 on record 5.
    std::uint32_t behaviorVersion{};
    bool oldCancelled{};
    // CommandRelease.
    std::int64_t releaseSeq{};
    std::int32_t tombstoneRanges{};
    std::vector<Release> releases;
    // EngineConfig (ledgerCapacity and behaviorVersion above are shared).
    std::uint32_t configVersion{};
    std::uint16_t maxWireVersion{};
    std::uint8_t decision{};
};
// Exact gate: schemaId 4, version 1, templates 1..5 with their fixed block lengths and frame
// lengths. Any other schema (3, 1) is a loud refusal: decode throws and the worker halts
// without checkpointing an event this binary cannot project.
Journal decode(std::span<const std::uint8_t> bytes);
// Replay transport rule: every journal record arrives as ONE unfragmented Aeron frame
// (BEGIN|END flags) within the schema 4 frame bound.
inline void admitFrame(std::uint8_t flags, std::size_t length) {
    if (flags != 0xC0 || length < 8 || length > MAX_JOURNAL_FRAME_LENGTH)
        throw std::runtime_error("Unexpected journal fragmentation/size");
}
std::string hex(std::span<const std::uint8_t> bytes);
}
