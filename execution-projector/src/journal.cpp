// SPDX-License-Identifier: Apache-2.0
#include "journal.hpp"
#include "oe_journal/JournalTrade.h"
#include "oe_journal/JournalTerminal.h"

namespace oe {
Journal decode(std::span<const std::uint8_t> b) {
    if (b.size() < 8) throw std::runtime_error("Truncated journal header");
    // SBE's shared encode/decode API takes char*. Getter-only use is read-only.
    auto* buffer = const_cast<char*>(reinterpret_cast<const char*>(b.data()));
    journal::MessageHeader header;
    header.wrap(buffer, 0, 1, b.size());
    if (header.schemaId() != 3 || header.version() != 1)
        throw std::runtime_error("Unsupported journal schema/version");
    Journal j; j.type = header.templateId();
    const auto block = header.blockLength();
    if ((j.type != 1 && j.type != 2) || block != (j.type == 1 ?
            journal::JournalTrade::sbeBlockLength() : journal::JournalTerminal::sbeBlockLength()) || b.size() != 8u + block)
        throw std::runtime_error("Unsupported journal template/length");
    if (j.type == 1) {
        journal::JournalTrade t; t.wrapForDecode(buffer, 8, block, header.version(), b.size());
        j.seq = t.egressSeq(); j.trade = t.tradeId(); j.market = t.marketId();
        j.taker = t.takerOrderId(); j.takerUser = t.takerUserId();
        j.maker = t.makerOrderId(); j.makerUser = t.makerUserId();
        j.price = t.price(); j.quantity = t.quantity();
        j.sideOrStatus = static_cast<std::uint8_t>(t.takerIsBuy()); j.timestamp = t.timestamp();
        j.takerOms = t.takerOmsOrderId(); j.makerOms = t.makerOmsOrderId();
        if (j.trade <= 0 || j.price <= 0 || j.quantity <= 0 || j.maker <= 0 ||
                j.makerOms <= 0 || j.makerUser <= 0 || j.sideOrStatus > 1)
            throw std::runtime_error("Invalid journal trade");
    } else {
        journal::JournalTerminal t; t.wrapForDecode(buffer, 8, block, header.version(), b.size());
        j.seq = t.egressSeq(); j.taker = t.orderId(); j.takerUser = t.userId();
        j.market = t.marketId(); j.sideOrStatus = static_cast<std::uint8_t>(t.status());
        j.timestamp = t.timestamp(); j.takerOms = t.omsOrderId();
        if (j.sideOrStatus < 2 || j.sideOrStatus > 4) throw std::runtime_error("Invalid terminal status");
    }
    if (j.seq < 0 || j.taker <= 0 || j.takerUser <= 0 || j.takerOms <= 0 || j.market <= 0 || j.timestamp < 0)
        throw std::runtime_error("Invalid journal identity");
    return j;
}
std::string hex(std::span<const std::uint8_t> bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result; result.reserve(bytes.size() * 2);
    for (auto b : bytes) { result += digits[b >> 4]; result += digits[b & 15]; }
    return result;
}
}
