// SPDX-License-Identifier: Apache-2.0
#include "journal.hpp"
#include "oe_journal/JournalTrade.h"
#include "oe_journal/JournalTerminal.h"
#include "oe_command/JournalCommandOutcome.h"

namespace oe {
Journal decode(std::span<const std::uint8_t> b) {
    if (b.size() < 8) throw std::runtime_error("Truncated journal header");
    // SBE's shared encode/decode API takes char*. Getter-only use is read-only.
    auto* buffer = const_cast<char*>(reinterpret_cast<const char*>(b.data()));
    journal::MessageHeader header;
    header.wrap(buffer, 0, 1, b.size());
    if (header.schemaId() == 1 && header.version() == 11 && header.templateId() == 28) {
        if (header.blockLength()!=command::JournalCommandOutcome::sbeBlockLength() ||
                b.size()!=8u+header.blockLength()) throw std::runtime_error("Invalid command outcome length");
        command::JournalCommandOutcome d; d.wrapForDecode(buffer,8,header.blockLength(),11,b.size());
        Journal j; j.type=28; j.seq=d.egressSeq(); j.commandHigh=d.commandIdHigh(); j.commandLow=d.commandIdLow();
        j.takerUser=d.userId(); j.takerOms=d.omsOrderId(); j.oldOrder=d.oldOrderId(); j.price=d.price();
        j.quantity=d.quantity(); j.budget=d.budget(); j.market=d.marketId(); j.kind=d.commandKind();
        j.orderType=d.orderType(); j.orderSide=d.orderSide(); j.appliedPosition=d.appliedPosition();
        j.timestamp=d.timestamp(); j.taker=d.orderId(); j.status=d.status(); j.reason=d.reason();
        j.oldCancelled=d.oldCancelled()!=0; j.result=d.result();
        if ((j.commandHigh==0 && j.commandLow==0) || j.takerUser<=0 || j.takerOms<=0 || j.market<=0 ||
                j.seq<0 || j.appliedPosition<0 || j.appliedPosition>j.seq || j.timestamp<0 || j.taker<0 ||
                j.kind>2 || j.orderType>2 || j.orderSide>1 || j.status<-1 || j.status>4 || j.reason<0 ||
                d.oldCancelled()>1 || j.result<0 || j.result>6 ||
                (j.kind==0 ? j.oldOrder!=0 : j.oldOrder<=0) ||
                (j.result<=1 ? j.taker<=0 || j.status<0 : j.taker!=0 || j.status!=-1) ||
                (j.oldCancelled && j.kind!=2)) throw std::runtime_error("Invalid command outcome");
        return j;
    }
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
