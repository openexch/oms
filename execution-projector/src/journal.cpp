// SPDX-License-Identifier: Apache-2.0
#include "journal.hpp"
#include "oe_mejournal/MessageHeader.h"
#include "oe_mejournal/GroupSizeEncoding.h"
#include "oe_mejournal/Trade.h"
#include "oe_mejournal/Terminal.h"
#include "oe_mejournal/CommandOutcome.h"
#include "oe_mejournal/CommandRelease.h"
#include "oe_mejournal/EngineConfig.h"

namespace oe {
namespace {
namespace mj = mejournal;
static_assert(mj::MessageHeader::encodedLength() == 8);
static_assert(mj::Trade::sbeSchemaId() == JOURNAL_SCHEMA_ID && mj::Trade::sbeSchemaVersion() == JOURNAL_SCHEMA_VERSION);
static_assert(mj::Trade::sbeTemplateId() == RECORD_TRADE && mj::Terminal::sbeTemplateId() == RECORD_TERMINAL &&
        mj::CommandOutcome::sbeTemplateId() == RECORD_OUTCOME && mj::CommandRelease::sbeTemplateId() == RECORD_RELEASE &&
        mj::EngineConfig::sbeTemplateId() == RECORD_CONFIG);
// Block lengths of contract section 2; a schema edit that moves them must fail the build.
static_assert(mj::Trade::sbeBlockLength() == 109 && mj::Terminal::sbeBlockLength() == 62 &&
        mj::CommandOutcome::sbeBlockLength() == 153 && mj::CommandRelease::sbeBlockLength() == 44 &&
        mj::CommandRelease::Releases::sbeBlockLength() == 22 && mj::EngineConfig::sbeBlockLength() == 39);
// Canonical outcome bytes: commandIdHigh through result (frame offsets), ending at timeInForce.
static_assert(mj::CommandOutcome::commandIdHighEncodingOffset() + 8 == OUTCOME_CANONICAL_BEGIN &&
        mj::CommandOutcome::timeInForceEncodingOffset() + 8 == OUTCOME_CANONICAL_END);
constexpr std::uint8_t REJECT_REASON_NULL = 255;

void fail(const char* why) { throw std::runtime_error(why); }
void requireLength(std::size_t actual, std::size_t expected, const char* record) {
    if (actual != expected) throw std::runtime_error(std::string("Invalid journal ") + record + " length");
}
}

Journal decode(std::span<const std::uint8_t> b) {
    if (b.size() < mj::MessageHeader::encodedLength()) fail("Truncated journal header");
    if (b.size() > MAX_JOURNAL_FRAME_LENGTH) fail("Journal frame exceeds 1112 bytes");
    // SBE's shared encode/decode API takes char*. Getter-only use is read-only.
    auto* buffer = const_cast<char*>(reinterpret_cast<const char*>(b.data()));
    mj::MessageHeader header(buffer, 0, b.size(), JOURNAL_SCHEMA_VERSION);
    const auto schema = header.schemaId(), version = header.version(), templateId = header.templateId();
    if (schema != JOURNAL_SCHEMA_ID || version != JOURNAL_SCHEMA_VERSION)
        throw std::runtime_error("Unsupported journal schema/version: schemaId=" + std::to_string(schema) +
                " version=" + std::to_string(version) + " templateId=" + std::to_string(templateId) +
                "; this binary decodes schemaId 4 version 1");
    if (templateId < RECORD_TRADE || templateId > RECORD_CONFIG)
        throw std::runtime_error("Unsupported journal template: templateId=" + std::to_string(templateId));
    const std::uint64_t block = header.blockLength();
    constexpr std::uint64_t head = mj::MessageHeader::encodedLength();
    Journal j; j.type = templateId;
    switch (templateId) {
    case RECORD_TRADE: {
        if (block != mj::Trade::sbeBlockLength()) fail("Invalid journal block length");
        requireLength(b.size(), head + block, "trade");
        mj::Trade t; t.wrapForDecode(buffer, head, block, version, b.size());
        j.journalSeq = t.journalSeq(); j.meEpoch = t.meEpoch(); j.egressSeq = t.egressSeq();
        j.trade = t.tradeId(); j.market = t.marketId();
        j.taker = t.takerOrderId(); j.takerUser = t.takerUserId();
        j.maker = t.makerOrderId(); j.makerUser = t.makerUserId();
        j.price = t.price(); j.quantity = t.quantity(); j.sideOrStatus = t.takerIsBuyRaw();
        j.timestamp = t.timestamp(); j.takerOms = t.takerOmsOrderId(); j.makerOms = t.makerOmsOrderId();
        if (j.trade <= 0 || j.price <= 0 || j.quantity <= 0 || j.market <= 0 || j.taker <= 0 || j.takerUser <= 0 ||
                j.takerOms <= 0 || j.maker <= 0 || j.makerUser <= 0 || j.makerOms <= 0 || j.sideOrStatus > 1)
            fail("Invalid journal trade");
        break;
    }
    case RECORD_TERMINAL: {
        if (block != mj::Terminal::sbeBlockLength()) fail("Invalid journal block length");
        requireLength(b.size(), head + block, "terminal");
        mj::Terminal t; t.wrapForDecode(buffer, head, block, version, b.size());
        j.journalSeq = t.journalSeq(); j.meEpoch = t.meEpoch(); j.egressSeq = t.egressSeq();
        j.taker = t.orderId(); j.takerUser = t.userId(); j.market = t.marketId();
        j.sideOrStatus = t.statusRaw(); j.timestamp = t.timestamp(); j.takerOms = t.omsOrderId();
        j.rejectReason = t.rejectReason();
        if (j.taker <= 0 || j.takerUser <= 0 || j.market <= 0 || j.takerOms <= 0 ||
                j.sideOrStatus < 2 || j.sideOrStatus > 4 || j.rejectReason == REJECT_REASON_NULL)
            fail("Invalid journal terminal");
        break;
    }
    case RECORD_OUTCOME: {
        if (block != mj::CommandOutcome::sbeBlockLength()) fail("Invalid journal block length");
        requireLength(b.size(), head + block, "command outcome");
        mj::CommandOutcome d; d.wrapForDecode(buffer, head, block, version, b.size());
        j.journalSeq = d.journalSeq(); j.meEpoch = d.meEpoch(); j.egressSeq = d.egressSeq();
        j.commandHigh = d.commandIdHigh(); j.commandLow = d.commandIdLow();
        j.takerUser = d.userId(); j.takerOms = d.omsOrderId(); j.oldOrder = d.oldOrderId(); j.price = d.price();
        j.quantity = d.quantity(); j.budget = d.budget(); j.market = d.marketId(); j.kind = d.commandKind();
        j.orderType = d.orderType(); j.orderSide = d.orderSide(); j.appliedPosition = d.appliedPosition();
        j.timestamp = d.timestamp(); j.taker = d.orderId(); j.status = d.status(); j.reason = d.reason();
        j.oldCancelled = d.oldCancelled() != 0; j.result = d.result();
        j.timeInForce = d.timeInForce(); j.expectedOldRemaining = d.expectedOldRemaining();
        j.ledgerSize = d.ledgerSize(); j.ledgerCapacity = d.ledgerCapacity(); j.behaviorVersion = d.behaviorVersion();
        if ((j.commandHigh == 0 && j.commandLow == 0) || j.takerUser <= 0 || j.takerOms <= 0 || j.market <= 0 ||
                j.appliedPosition < 0 || j.appliedPosition > j.egressSeq || j.taker < 0 ||
                j.kind > 2 || j.orderType > 2 || j.orderSide > 1 || j.status < -1 || j.status > 4 || j.reason < 0 ||
                d.oldCancelled() > 1 || j.result < 0 || j.result > 8 ||
                (j.kind == 0 ? j.oldOrder != 0 : j.oldOrder <= 0) ||
                (j.result <= 1 ? j.taker <= 0 || j.status < 0 : j.taker != 0 || j.status != -1) ||
                (j.oldCancelled && j.kind != 2) || j.timeInForce > 2 ||
                j.expectedOldRemaining < 0 || (j.kind != 2 && j.expectedOldRemaining != 0) ||
                j.ledgerSize < 0 || j.ledgerCapacity < 0 || j.behaviorVersion < 1)
            fail("Invalid command outcome");
        break;
    }
    case RECORD_RELEASE: {
        constexpr std::uint64_t entryBlock = mj::CommandRelease::Releases::sbeBlockLength();
        constexpr std::uint64_t groupHead = mj::CommandRelease::Releases::sbeHeaderSize();
        if (block != mj::CommandRelease::sbeBlockLength()) fail("Invalid journal block length");
        if (b.size() < head + block + groupHead) fail("Invalid journal command release length");
        mj::GroupSizeEncoding dimensions(buffer, head + block, b.size(), version);
        const std::uint64_t count = dimensions.numInGroup();
        if (dimensions.blockLength() != entryBlock) fail("Invalid journal release entry block length");
        if (count > MAX_RELEASE_ENTRIES) fail("Journal command release exceeds 48 entries");
        requireLength(b.size(), head + block + groupHead + entryBlock * count, "command release");
        mj::CommandRelease r; r.wrapForDecode(buffer, head, block, version, b.size());
        j.journalSeq = r.journalSeq(); j.meEpoch = r.meEpoch(); j.egressSeq = r.egressSeq();
        j.releaseSeq = r.releaseSeq(); j.ledgerSize = r.ledgerSize(); j.ledgerCapacity = r.ledgerCapacity();
        j.tombstoneRanges = r.tombstoneRanges();
        if (j.releaseSeq < 0 || j.ledgerSize < 0 || j.ledgerCapacity < 0 || j.tombstoneRanges < 0)
            fail("Invalid journal command release");
        auto& group = r.releases();
        j.releases.reserve(count);
        while (group.hasNext()) {
            group.next();
            Release e{group.commandIdHigh(), group.idLowFloor(), group.scopeRaw(), group.resultRaw(), group.removed()};
            if (e.scope > 1 || e.result > 2 || e.removed < 0) fail("Invalid journal release entry");
            j.releases.push_back(e);
        }
        if (j.releases.size() != count || r.sbePosition() != b.size()) fail("Invalid journal command release length");
        break;
    }
    default: { // RECORD_CONFIG
        if (block != mj::EngineConfig::sbeBlockLength()) fail("Invalid journal block length");
        requireLength(b.size(), head + block, "engine config");
        mj::EngineConfig c; c.wrapForDecode(buffer, head, block, version, b.size());
        j.journalSeq = c.journalSeq(); j.meEpoch = c.meEpoch(); j.egressSeq = c.egressSeq();
        j.configVersion = c.configVersion(); j.behaviorVersion = c.behaviorVersion();
        j.ledgerCapacity = c.ledgerCapacity(); j.maxWireVersion = c.maxWireVersion(); j.decision = c.decisionRaw();
        if (j.configVersion < 1 || j.behaviorVersion < 1 || j.ledgerCapacity < 1 || j.maxWireVersion < 11 ||
                j.decision > 3)
            fail("Invalid journal engine config");
        break;
    }
    }
    // RC2 S3 (contract 10.1): every record of a behaviour-2 ME carries journalSeq >= 1 and
    // meEpoch >= 1 (the bound genesis); 0 is a behaviour-1 or pre-S3 producer, refused loudly.
    // Density and the single epoch are cursor rules (store.cpp), not frame rules.
    if (j.journalSeq < 1 || j.meEpoch < 1) fail("Invalid journal sequence/epoch");
    if (j.egressSeq < 0 || j.timestamp < 0)
        fail("Invalid journal identity");
    return j;
}
std::string hex(std::span<const std::uint8_t> bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result; result.reserve(bytes.size() * 2);
    for (auto b : bytes) { result += digits[b >> 4]; result += digits[b & 15]; }
    return result;
}
}
