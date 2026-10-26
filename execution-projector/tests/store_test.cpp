// SPDX-License-Identifier: Apache-2.0
#include "store.hpp"
#include <cstdlib>
#include <initializer_list>
#include <tuple>
#include <fstream>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

namespace {
void check(bool ok, const char* why) { if (!ok) throw std::runtime_error(why); }
template<class F> void rejects(F&& action, const char* why) {
    bool rejected = false;
    try { action(); } catch (const std::exception&) { rejected = true; }
    check(rejected, why);
}
void put(std::vector<std::uint8_t>& b, int p, std::uint64_t v, int size) {
    for (int i = 0; i < size; ++i) b.at(p + i) = (v >> (i * 8)) & 255;
}
std::uint64_t get(const std::vector<std::uint8_t>& b, int p, int size) {
    std::uint64_t v = 0;
    for (int i = 0; i < size; ++i) v |= static_cast<std::uint64_t>(b.at(p + i)) << (i * 8);
    return v;
}
void header(std::vector<std::uint8_t>& b, int block, int templateId, int schema, int version) {
    put(b, 0, block, 2); put(b, 2, templateId, 2); put(b, 4, schema, 2); put(b, 6, version, 2);
}
std::string message(const std::vector<std::uint8_t>& bytes) {
    try { oe::decode(bytes); } catch (const std::exception& e) { return e.what(); }
    return "accepted";
}
// The six schema 4 frames from match's MeJournalFixtureDump (hashes pinned in SHA256SUMS).
oe::Event fixture(const char* name, std::int64_t position) {
    std::ifstream in(std::string(OE_FIXTURE_DIR) + "/java-" + name + ".bin", std::ios::binary);
    check(in.good(), "Missing Java ME journal fixture");
    return {position, std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in), {})};
}
// Schema 4 frame offsets (frame = 8 header + block offset), contract section 2.
constexpr int TRADE_ID = 32, TRADE_TAKER_IS_BUY = 92, TRADE_PRICE = 76;
constexpr int TERMINAL_STATUS = 52, TERMINAL_REJECT = 69;
constexpr int OUT_EGRESS = 24, OUT_LOW = 40, OUT_PRICE = 72, OUT_KIND = 100, OUT_ORDER = 119, OUT_STATUS = 127,
        OUT_RESULT = 136, OUT_TIF = 140, OUT_EOR = 141, OUT_LEDGER = 149, OUT_BEHAVIOR = 157;
constexpr int REL_GROUP = 52, REL_ENTRIES = 56;
constexpr int CFG_VERSION = 32, CFG_CAPACITY = 40, CFG_WIRE = 44, CFG_DECISION = 46;
// RC2 S3: every fixture carries meEpoch G; the projector cursors on a dense journalSeq.
constexpr std::int64_t G = 0x123456789ABCDEF1LL;
constexpr int SEQ = 8, EPOCH = 16;
oe::Event at(oe::Event e, std::int64_t seq, std::int64_t position) {
    put(e.bytes, SEQ, static_cast<std::uint64_t>(seq), 8); e.position = position; return e;
}
oe::Event trade(std::int64_t id, std::int64_t seq, std::int64_t position) {
    auto e = at(fixture("trade", position), seq, position); put(e.bytes, TRADE_ID, id, 8); return e;
}
oe::Event observation(const oe::Event& outcome, int result, std::int64_t egress, std::int64_t position) {
    auto e = outcome; e.position = position;
    put(e.bytes, OUT_EGRESS, egress, 8); put(e.bytes, OUT_ORDER, 0, 8);
    put(e.bytes, OUT_STATUS, static_cast<std::uint32_t>(-1), 4); put(e.bytes, OUT_RESULT, result, 4);
    return e;
}
oe::Event release(std::size_t entries, std::int64_t position) {
    oe::Event e{position, std::vector<std::uint8_t>(REL_ENTRIES + 22 * entries)}; auto& b = e.bytes;
    header(b, 44, 4, 4, 1); put(b, SEQ, 6, 8); put(b, EPOCH, static_cast<std::uint64_t>(G), 8); put(b, 24, 640, 8); put(b, 32, 7, 8); put(b, 40, 1, 4); put(b, 44, 100000, 4);
    put(b, 48, 1, 4); put(b, REL_GROUP, 22, 2); put(b, REL_GROUP + 2, entries, 2);
    for (std::size_t i = 0; i < entries; ++i) {
        const int p = REL_ENTRIES + 22 * static_cast<int>(i);
        put(b, p, 17, 8); put(b, p + 8, i + 1, 8); put(b, p + 16, i % 2, 1); put(b, p + 17, i % 3, 1); put(b, p + 18, 1, 4);
    }
    return e;
}
// Schema 3 v1 JournalTrade (block 93) and terminal (block 45), the pre-S1 ME journal.
std::vector<std::uint8_t> schema3Trade() {
    std::vector<std::uint8_t> b(101); header(b, 93, 1, 3, 1);
    put(b, 8, 10000, 8); put(b, 16, 1, 8); put(b, 24, 1, 4); put(b, 28, 91, 8); put(b, 36, 11, 8);
    put(b, 44, 92, 8); put(b, 52, 12, 8); put(b, 60, 12345678901234, 8); put(b, 68, 123456789, 8);
    put(b, 76, 1, 1); put(b, 77, 1700000000123, 8); put(b, 85, 101, 8); put(b, 93, 102, 8);
    return b;
}
std::vector<std::uint8_t> schema3Terminal() {
    std::vector<std::uint8_t> b(53); header(b, 45, 2, 3, 1);
    put(b, 8, 10000, 8); put(b, 16, 91, 8); put(b, 24, 11, 8); put(b, 32, 1, 4); put(b, 36, 2, 1);
    put(b, 37, 1700000000123, 8); put(b, 45, 101, 8);
    return b;
}
// Order schema 1 v11 template 28 JournalCommandOutcome (block 116), the pre-S1 outcome frame.
std::vector<std::uint8_t> schema1Outcome() {
    std::vector<std::uint8_t> b(124); header(b, 116, 28, 1, 11);
    put(b, 8, 128, 8); put(b, 16, 17, 8); put(b, 24, 1, 8); put(b, 32, 100, 8); put(b, 40, 9001, 8);
    return b;
}
std::string refusal(int schema, int version, int templateId) {
    return "Unsupported journal schema/version: schemaId=" + std::to_string(schema) + " version=" +
           std::to_string(version) + " templateId=" + std::to_string(templateId) +
           "; this binary decodes schemaId 4 version 1";
}
void decodeTests() {
    // decodesEveryRecordFromJavaFixtures
    // RC2 S3: one identity-bound producer sequence, journalSeq 1..6, meEpoch G, egressSeq non-decreasing.
    auto t = oe::decode(fixture("trade", 0).bytes);
    check(t.type == 1 && t.journalSeq == 2 && t.meEpoch == G && t.egressSeq == 128 && t.trade == 1 &&
          t.market == 1 && t.taker == 91 && t.takerUser == 11 && t.maker == 92 && t.makerUser == 12 &&
          t.price == 12345678901234 && t.quantity == 123456789 && t.sideOrStatus == 1 &&
          t.timestamp == 1700000000123 && t.takerOms == 101 && t.makerOms == 102, "Java trade fixture mismatch");
    auto e = oe::decode(fixture("terminal", 0).bytes);
    check(e.type == 2 && e.journalSeq == 3 && e.meEpoch == G && e.egressSeq == 128 && e.taker == 91 && e.takerUser == 11 && e.market == 1 &&
          e.sideOrStatus == 2 && e.timestamp == 1700000000123 && e.takerOms == 101 && e.rejectReason == 0,
          "Java terminal fixture mismatch");
    auto o = oe::decode(fixture("command.first", 0).bytes);
    check(o.type == 3 && o.journalSeq == 4 && o.meEpoch == G && o.egressSeq == 192 && o.commandHigh == 17 && o.commandLow == 1 && o.takerUser == 100 &&
          o.takerOms == 9001 && o.oldOrder == 0 && o.price == 5900000000000 && o.quantity == 100000000 &&
          o.budget == 0 && o.market == 1 && o.kind == 0 && o.orderType == 0 && o.orderSide == 0 &&
          o.appliedPosition == 192 && o.timestamp == 1000 && o.taker == 1 && o.status == 0 && o.reason == 0 &&
          !o.oldCancelled && o.result == 0 && o.timeInForce == 0 && o.expectedOldRemaining == 0 &&
          o.ledgerSize == 1 && o.ledgerCapacity == 100000 && o.behaviorVersion == 2, "Java command fixture mismatch");
    // A behaviour-2 ME never re-journals an equal duplicate (X4): the sixth fixture is a CONFLICT
    // of the same command id in the commandRejection shape.
    auto r = oe::decode(fixture("command.conflict", 0).bytes);
    check(r.type == 3 && r.journalSeq == 5 && r.meEpoch == G && r.egressSeq == 256 && r.appliedPosition == 256 &&
          r.commandHigh == 17 && r.commandLow == 1 && r.price == 6000000000000 && r.taker == 0 && r.status == -1 &&
          r.reason == 0 && r.result == 2 && r.ledgerSize == 1 && r.behaviorVersion == 2,
          "Java conflict fixture mismatch");
    auto l = oe::decode(fixture("release", 0).bytes);
    check(l.type == 4 && l.journalSeq == 6 && l.meEpoch == G && l.egressSeq == 640 && l.releaseSeq == 7 && l.ledgerSize == 1 && l.ledgerCapacity == 100000 &&
          l.tombstoneRanges == 1 && l.releases.size() == 2 &&
          l.releases[0].commandHigh == 17 && l.releases[0].idLowFloor == 2 && l.releases[0].scope == 0 &&
          l.releases[0].result == 0 && l.releases[0].removed == 1 &&
          l.releases[1].commandHigh == 17 && l.releases[1].idLowFloor == 5 && l.releases[1].scope == 1 &&
          l.releases[1].result == 1 && l.releases[1].removed == 0, "Java release fixture mismatch");
    auto c = oe::decode(fixture("config", 0).bytes);
    check(c.type == 5 && c.journalSeq == 1 && c.meEpoch == G && c.egressSeq == 64 && c.configVersion == 1 &&
          c.behaviorVersion == 2 &&
          c.ledgerCapacity == 100000 && c.maxWireVersion == 12 && c.decision == 0, "Java config fixture mismatch");
    for (const char* name : {"trade", "terminal", "command.first", "command.conflict", "release", "config"}) {
        auto f = fixture(name, 0);
        for (std::size_t size = 0; size < f.bytes.size(); ++size)
            rejects([&] { oe::decode(std::span(f.bytes).first(size)); }, "Truncated frame accepted");
        auto longer = f.bytes; longer.push_back(0);
        rejects([&] { oe::decode(longer); }, "Frame with trailing bytes accepted");
        auto block = f.bytes; put(block, 0, get(block, 0, 2) + 1, 2);
        check(message(block).starts_with("Invalid journal block length"), "decodeRefusesWrongBlock");
        block = f.bytes; put(block, 0, get(block, 0, 2) - 1, 2);
        check(message(block).starts_with("Invalid journal block length"), "decodeRefusesWrongBlock (short)");
        auto negative = f.bytes; put(negative, 8, static_cast<std::uint64_t>(-1), 8);
        rejects([&] { oe::decode(negative); }, "Negative journalSeq accepted");
        negative = f.bytes; put(negative, 16, static_cast<std::uint64_t>(-1), 8);
        rejects([&] { oe::decode(negative); }, "Negative meEpoch accepted");
        negative = f.bytes; put(negative, 24, static_cast<std::uint64_t>(-1), 8);
        rejects([&] { oe::decode(negative); }, "Negative egressSeq accepted");
        auto seq = f.bytes; put(seq, 8, 5, 8); put(seq, 16, 3, 8);
        check(oe::decode(seq).journalSeq == 5 && oe::decode(seq).meEpoch == 3, "journalSeq/meEpoch >= 1 refused");
    }
    // decodeRefusesSchema3Loudly / decodeRefusesSchema1Template28 / decodeRefusesSchema4Version2
    check(message(schema3Trade()) == refusal(3, 1, 1), "decodeRefusesSchema3Loudly (trade)");
    check(message(schema3Terminal()) == refusal(3, 1, 2), "decodeRefusesSchema3Loudly (terminal)");
    check(message(schema1Outcome()) == refusal(1, 11, 28), "decodeRefusesSchema1Template28");
    for (const char* name : {"trade", "terminal", "command.first", "release", "config"}) {
        auto v2 = fixture(name, 0).bytes; put(v2, 6, 2, 2);
        check(message(v2) == refusal(4, 2, static_cast<int>(get(v2, 2, 2))), "decodeRefusesSchema4Version2");
        auto v0 = fixture(name, 0).bytes; put(v0, 6, 0, 2);
        check(message(v0) == refusal(4, 0, static_cast<int>(get(v0, 2, 2))), "Schema 4 version 0 accepted");
    }
    // decodeRefusesTemplate6
    for (int templateId : {0, 6, 28}) {
        auto other = fixture("config", 0).bytes; put(other, 2, templateId, 2);
        check(message(other).starts_with("Unsupported journal template"), "decodeRefusesTemplate6");
    }
    auto outcome = fixture("command.first", 0);
    // resultSevenAndEightAccepted / resultNineRefused
    for (int result : {2, 3, 4, 5, 6, 7, 8})
        check(oe::decode(observation(outcome, result, 256, 0).bytes).result == result, "resultSevenAndEightAccepted");
    rejects([&] { oe::decode(observation(outcome, 9, 256, 0).bytes); }, "resultNineRefused");
    rejects([&] { oe::decode(observation(outcome, -1, 256, 0).bytes); }, "Negative result accepted");
    auto applied = observation(outcome, 7, 256, 0); put(applied.bytes, OUT_ORDER, 1, 8);
    rejects([&] { oe::decode(applied.bytes); }, "Result 7 with an order id accepted");
    auto bad = outcome.bytes; put(bad, OUT_TIF, 3, 1);
    rejects([&] { oe::decode(bad); }, "timeInForce 3 (GTD) accepted");
    bad = outcome.bytes; put(bad, OUT_EOR, 5, 8);
    rejects([&] { oe::decode(bad); }, "expectedOldRemaining on a CREATE accepted");
    bad = outcome.bytes; put(bad, OUT_BEHAVIOR, 0, 4);
    rejects([&] { oe::decode(bad); }, "behaviorVersion 0 accepted");
    bad = outcome.bytes; put(bad, OUT_LEDGER, static_cast<std::uint32_t>(-1), 4);
    rejects([&] { oe::decode(bad); }, "Negative ledgerSize accepted");
    bad = outcome.bytes; put(bad, OUT_KIND, 3, 1);
    rejects([&] { oe::decode(bad); }, "Command kind 3 accepted");
    // terminalRejectReason255Refused
    auto terminal = fixture("terminal", 0);
    bad = terminal.bytes; put(bad, TERMINAL_REJECT, 255, 1);
    rejects([&] { oe::decode(bad); }, "terminalRejectReason255Refused");
    bad = terminal.bytes; put(bad, TERMINAL_STATUS, 4, 1); put(bad, TERMINAL_REJECT, 7, 1);
    check(oe::decode(bad).rejectReason == 7 && oe::decode(bad).sideOrStatus == 4, "ME reject reason not verbatim");
    for (int status : {0, 1, 5}) {
        bad = terminal.bytes; put(bad, TERMINAL_STATUS, status, 1);
        rejects([&] { oe::decode(bad); }, "Terminal status outside 2..4 accepted");
    }
    auto trade1 = fixture("trade", 0);
    bad = trade1.bytes; put(bad, TRADE_TAKER_IS_BUY, 2, 1);
    rejects([&] { oe::decode(bad); }, "takerIsBuy 2 accepted");
    bad = trade1.bytes; put(bad, TRADE_ID, 0, 8);
    rejects([&] { oe::decode(bad); }, "Trade id 0 accepted");
    // release49EntriesRefused / frameAbove1112Refused
    check(oe::MAX_JOURNAL_FRAME_LENGTH == 1112, "MAX_JOURNAL_FRAME_LENGTH is not 1112");
    auto full = release(48, 0);
    check(full.bytes.size() == oe::MAX_JOURNAL_FRAME_LENGTH && oe::decode(full.bytes).releases.size() == 48,
          "48-entry release at the frame bound refused");
    check(oe::decode(release(0, 0).bytes).releases.empty(), "Empty release refused");
    check(message(release(49, 0).bytes) == "Journal frame exceeds 1112 bytes", "release49EntriesRefused");
    auto counted = full; put(counted.bytes, REL_GROUP + 2, 49, 2);
    check(message(counted.bytes) == "Journal command release exceeds 48 entries", "numInGroup 49 accepted");
    std::vector<std::uint8_t> above(oe::MAX_JOURNAL_FRAME_LENGTH + 1); header(above, 44, 4, 4, 1);
    check(message(above) == "Journal frame exceeds 1112 bytes", "frameAbove1112Refused");
    auto entryBlock = release(2, 0); put(entryBlock.bytes, REL_GROUP, 21, 2);
    rejects([&] { oe::decode(entryBlock.bytes); }, "Release entry block 21 accepted");
    auto scope = release(1, 0); put(scope.bytes, REL_ENTRIES + 16, 2, 1);
    rejects([&] { oe::decode(scope.bytes); }, "Release scope 2 accepted");
    auto relResult = release(1, 0); put(relResult.bytes, REL_ENTRIES + 17, 3, 1);
    rejects([&] { oe::decode(relResult.bytes); }, "Release result 3 accepted");
    auto removed = release(1, 0); put(removed.bytes, REL_ENTRIES + 18, static_cast<std::uint32_t>(-1), 4);
    rejects([&] { oe::decode(removed.bytes); }, "Negative removed accepted");
    auto counters = release(1, 0); put(counters.bytes, 48, static_cast<std::uint32_t>(-1), 4);
    rejects([&] { oe::decode(counters.bytes); }, "Negative tombstoneRanges accepted");
    auto config = fixture("config", 0);
    for (auto [offset, value, size] : std::initializer_list<std::tuple<int, int, int>>{
            {CFG_VERSION, 0, 4}, {CFG_CAPACITY, 0, 4}, {CFG_WIRE, 10, 2}, {CFG_DECISION, 4, 1}, {36, 0, 4}}) {
        bad = config.bytes; put(bad, offset, value, size);
        rejects([&] { oe::decode(bad); }, "Invalid engine config accepted");
    }
    // fragmentedFrameRefused (the main.cpp replay rule)
    oe::admitFrame(0xC0, 8); oe::admitFrame(0xC0, oe::MAX_JOURNAL_FRAME_LENGTH);
    for (std::uint8_t flags : {0x80, 0x40, 0x00, 0xE0})
        rejects([&] { oe::admitFrame(flags, 117); }, "fragmentedFrameRefused");
    rejects([&] { oe::admitFrame(0xC0, oe::MAX_JOURNAL_FRAME_LENGTH + 1); }, "Replay frame above 1112 admitted");
    rejects([&] { oe::admitFrame(0xC0, 7); }, "Replay frame below the header admitted");
}
struct Db {
    PGconn* c;
    explicit Db(const std::string& url) : c(PQconnectdb(url.c_str())) { check(PQstatus(c) == CONNECTION_OK, "Test DB unavailable"); }
    ~Db() { PQfinish(c); }
    oe::Result sql(const std::string& sql) {
        oe::Result r(PQexec(c, sql.c_str()));
        if (!r || (PQresultStatus(r.get()) != PGRES_COMMAND_OK && PQresultStatus(r.get()) != PGRES_TUPLES_OK))
            throw std::runtime_error("Test setup SQL failed");
        return r;
    }
    long scalar(const std::string& sql) { auto r = this->sql(sql); return std::stol(PQgetvalue(r.get(), 0, 0)); }
};
// ---- RC2 S3 contract section 10: journalSeq >= 1 and dense, one epoch, --rebase-source ----
int s3Failures = 0;
template<class F> void s3case(const char* name, F&& body) {
    try { body(); std::cout << "PASS " << name << '\n'; }
    catch (const std::exception& e) { ++s3Failures; std::cout << "FAIL " << name << ": " << e.what() << '\n'; }
}
template<class F> std::string error(F&& action) {
    try { action(); } catch (const std::exception& e) { return e.what(); }
    return "accepted";
}
const std::string FIRST = "First journal record must be EngineConfig FRESH at journalSeq 1; checkpoint not advanced";
const std::string GAP = "Journal sequence gap; checkpoint not advanced";
const std::string EPOCH_MISMATCH = "Journal epoch mismatch; checkpoint not advanced";
const std::string REGRESSION = "Journal sequence regression; checkpoint not advanced";
oe::Event cfg(std::int64_t position) { return at(fixture("config", 0), 1, position); }
oe::Event trd(std::int64_t seq, std::int64_t position) { return at(fixture("trade", 0), seq, position); }
oe::Event term(std::int64_t seq, std::int64_t position) { return at(fixture("terminal", 0), seq, position); }
oe::Event outc(std::int64_t seq, std::int64_t position) { return at(fixture("command.first", 0), seq, position); }
void apply(oe::Store& s, std::initializer_list<oe::Event> events) {
    std::vector<oe::Event> batch(events); s.apply(batch);
}
// An isolated schema with the projector and writer-ownership migrations, owner archive at epoch 2.
std::string isolatedSchema(const std::string& url, const std::string& schema) {
    Db db(url); db.sql("CREATE SCHEMA " + schema); db.sql("SET search_path TO " + schema);
    db.sql("CREATE TABLE orders(oms_order_id bigint PRIMARY KEY); INSERT INTO orders VALUES(101),(102);"
        "CREATE TABLE executions(execution_id bigserial PRIMARY KEY, trade_id bigint, oms_order_id bigint REFERENCES orders,"
        "user_id bigint, market_id int, side text, price bigint, quantity bigint, is_maker boolean, executed_at timestamptz)");
    std::ifstream input(OE_PROJECTOR_SCHEMA); std::string sql((std::istreambuf_iterator<char>(input)), {});
    db.sql(sql);
    std::ifstream ownership(OE_WRITER_SCHEMA); std::string fenceSql((std::istreambuf_iterator<char>(ownership)), {});
    db.sql(fenceSql);
    db.sql("SELECT transition_execution_writer('legacy',1,'paused','s3 quiesce'); "
           "SELECT transition_execution_writer('paused',2,'archive','s3 activation')");
    return url + " options='-c search_path=" + schema + "'";
}
long cursor(Db& db, const char* column) {
    return db.scalar(std::string("SELECT ") + column + " FROM execution_projector_checkpoint");
}
void s3DecodeCases() {
    s3case("refusesJournalSeqOrEpochZero", [] {
        for (const char* name : {"trade", "terminal", "command.first", "command.conflict", "release", "config"}) {
            auto f = fixture(name, 0);
            check(oe::decode(f.bytes).journalSeq >= 1 && oe::decode(f.bytes).meEpoch == G, "fixture not identity-bound");
            for (std::int64_t bad : {std::int64_t{0}, std::int64_t{-1}}) {
                auto seq = f.bytes; put(seq, SEQ, static_cast<std::uint64_t>(bad), 8);
                check(message(seq) == "Invalid journal sequence/epoch", "journalSeq < 1 accepted");
                auto epoch = f.bytes; put(epoch, EPOCH, static_cast<std::uint64_t>(bad), 8);
                check(message(epoch) == "Invalid journal sequence/epoch", "meEpoch < 1 accepted");
            }
        }
    });
}
void s3StoreCases(const std::string& url) {
    const std::string base = "projector_s3_" + std::to_string(getpid());
    s3case("firstRecordMustBeFreshSeqOne", [&] {
        auto u = isolatedSchema(url, base + "_first");
        oe::Store s(u, "node0-rec1", "rec-A"); Db db(u);
        check(error([&] { apply(s, {trd(1, 160)}); }) == FIRST, "a trade as the first record accepted");
        auto bump = cfg(64); put(bump.bytes, CFG_DECISION, 3, 1);
        check(error([&] { apply(s, {bump}); }) == FIRST, "a BUMP as the first record accepted");
        check(error([&] { apply(s, {at(fixture("config", 0), 2, 64)}); }) == FIRST, "FRESH at journalSeq 2 accepted");
        check(cursor(db, "position") == 0 && cursor(db, "last_journal_seq") == 0 && cursor(db, "me_epoch") == 0,
              "a refused first record advanced the checkpoint");
        check(db.scalar("SELECT count(*) FROM execution_journal_events") == 0, "a refused first record left a row");
        apply(s, {cfg(64)});
        check(cursor(db, "position") == 64 && cursor(db, "last_journal_seq") == 1 && cursor(db, "me_epoch") == G,
              "FRESH at journalSeq 1 did not bind the cursor");
        check(s.lastJournalSeq() == 1 && s.meEpoch() == G, "in-memory cursor differs from the checkpoint");
    });
    s3case("refusesSequenceGapWithoutAdvancing", [&] {
        auto u = isolatedSchema(url, base + "_gap");
        oe::Store s(u, "node0-rec1", "rec-A"); Db db(u);
        apply(s, {cfg(64)});
        check(error([&] { apply(s, {trd(3, 160)}); }) == GAP, "a journalSeq gap accepted");
        check(error([&] { apply(s, {trd(2, 160), term(4, 256)}); }) == GAP, "a gap inside a batch accepted");
        check(cursor(db, "position") == 64 && cursor(db, "last_journal_seq") == 1, "a gap advanced the checkpoint");
        check(db.scalar("SELECT count(*) FROM executions") == 0, "a refused batch committed executions");
        apply(s, {trd(2, 160), term(3, 256)});
        check(cursor(db, "position") == 256 && cursor(db, "last_journal_seq") == 3, "dense seqs not applied");
    });
    s3case("refusesEpochChange", [&] {
        auto u = isolatedSchema(url, base + "_epoch");
        oe::Store s(u, "node0-rec1", "rec-A"); Db db(u);
        apply(s, {cfg(64)});
        auto other = term(2, 160); put(other.bytes, EPOCH, static_cast<std::uint64_t>(G + 1), 8);
        check(error([&] { apply(s, {other}); }) == EPOCH_MISMATCH, "a second epoch accepted");
        check(cursor(db, "position") == 64 && cursor(db, "last_journal_seq") == 1 && cursor(db, "me_epoch") == G,
              "an epoch mismatch advanced the checkpoint");
    });
    s3case("refusesSequenceRegressionOnOneRecording", [&] {
        auto u = isolatedSchema(url, base + "_regression");
        oe::Store s(u, "node0-rec1", "rec-A"); Db db(u);
        apply(s, {cfg(64), trd(2, 160), term(3, 256)});
        check(error([&] { apply(s, {term(2, 288)}); }) == REGRESSION, "a regressed journalSeq accepted");
        check(error([&] { apply(s, {term(3, 288)}); }) == REGRESSION, "a repeated journalSeq at a new position accepted");
        check(cursor(db, "position") == 256 && cursor(db, "last_journal_seq") == 3, "a regression advanced the checkpoint");
    });
    s3case("checkpointCarriesLastJournalSeqAndEpoch", [&] {
        auto u = isolatedSchema(url, base + "_checkpoint");
        {
            oe::Store s(u, "node0-rec1", "rec-A");
            apply(s, {cfg(64), trd(2, 160)});
        }
        oe::Store restarted(u, "node0-rec1", "rec-A");
        check(restarted.lastJournalSeq() == 2 && restarted.meEpoch() == G, "restart lost the seq/epoch cursor");
        Db db(u);
        check(cursor(db, "position") == 160 && cursor(db, "last_journal_seq") == 2 && cursor(db, "me_epoch") == G &&
              cursor(db, "last_trade_id") == 1, "checkpoint row lacks the seq/epoch cursor");
        check(error([&] { apply(restarted, {trd(2, 192)}); }) == REGRESSION, "restart forgot the seq cursor");
        apply(restarted, {term(3, 192)});
        check(cursor(db, "last_journal_seq") == 3, "the next seq after a restart did not apply");
    });
    s3case("positionRedeliveryStaysIdempotent", [&] {
        auto u = isolatedSchema(url, base + "_redelivery");
        oe::Store s(u, "node0-rec1", "rec-A"); Db db(u);
        apply(s, {cfg(64), trd(2, 160), term(3, 256)});
        apply(s, {cfg(64), trd(2, 160), term(3, 256)});
        check(cursor(db, "position") == 256 && db.scalar("SELECT count(*) FROM execution_journal_events") == 3 &&
              db.scalar("SELECT count(*) FROM executions") == 2 &&
              db.scalar("SELECT count(*) FROM execution_journal_terminals") == 1, "position redelivery was not a no-op");
        check(s.duplicateSeqs() == 0, "position redelivery counted as a duplicate journalSeq");
    });
    s3case("rebasedSourceSkipsAlreadyProjectedSeqs", [&] {
        auto u = isolatedSchema(url, base + "_rebase");
        Db db(u);
        {
            oe::Store a(u, "node0-rec1", "rec-A");
            apply(a, {cfg(64), trd(2, 160), term(3, 256), outc(4, 352)});
            check(error([&] { oe::Store::rebase(u, "node1-rec1", "rec-B"); }) != "accepted",
                  "rebase accepted while a worker holds the checkpoint");
        }
        oe::Store::rebase(u, "node1-rec1", "rec-B");
        check(db.scalar("SELECT count(*) FROM execution_projector_checkpoint WHERE source_identity='node1-rec1' "
                        "AND recording_descriptor='rec-B'") == 1, "rebase did not replace source and descriptor");
        check(cursor(db, "position") == 0 && cursor(db, "last_journal_seq") == 4 && cursor(db, "me_epoch") == G &&
              cursor(db, "last_trade_id") == 1, "rebase did not reset position and keep the seq/epoch/trade cursors");
        rejects([&] { oe::Store old(u, "node0-rec1", "rec-A"); }, "the old source still opens after a rebase");
        oe::Store b(u, "node1-rec1", "rec-B");
        // Member B's journal: the same frames (same seqs, byte-identical) at B's own positions, then seq 5.
        apply(b, {cfg(96), trd(2, 192), term(3, 288), outc(4, 384), at(fixture("command.conflict", 0), 5, 480)});
        check(cursor(db, "position") == 480 && cursor(db, "last_journal_seq") == 5, "the first new seq did not apply");
        check(db.scalar("SELECT count(*) FROM execution_journal_events WHERE source_identity='node1-rec1'") == 1,
              "already projected seqs inserted raw events again");
        check(db.scalar("SELECT count(*) FROM executions") == 2 &&
              db.scalar("SELECT count(*) FROM execution_journal_terminals") == 1 &&
              db.scalar("SELECT count(*) FROM me_command_outcomes") == 1, "already projected seqs projected twice");
        check(b.duplicateSeqs() == 4, "duplicate journalSeqs not counted");
        // Past the rebase range, a repeat is a regression again.
        check(error([&] { apply(b, {term(3, 512)}); }) == REGRESSION, "a regression accepted after the rebase range");
        rejects([&] { oe::Store::rebase(u, "node0-rec1", "rec-A2"); },
                "rebase onto a source identity that already has journal rows accepted");
    });
    s3case("rebasedSourceRefusesConflictingRepeatedTrade", [&] {
        auto u = isolatedSchema(url, base + "_rebase_conflict");
        Db db(u);
        { oe::Store a(u, "node0-rec1", "rec-A"); apply(a, {cfg(64), trd(2, 160)}); }
        oe::Store::rebase(u, "node1-rec1", "rec-B");
        oe::Store b(u, "node1-rec1", "rec-B");
        auto changed = trd(2, 192); put(changed.bytes, TRADE_PRICE, 5, 8);
        check(error([&] { apply(b, {cfg(96), changed}); }) == "Conflicting repeated trade",
              "a different trade at an already projected seq accepted");
        check(cursor(db, "position") == 0 && cursor(db, "last_journal_seq") == 2, "a conflicting repeat advanced the checkpoint");
    });
    // Verifier additions (RC2 S3 review): a rebase never turns a foreign epoch or a hole into a skip.
    s3case("verifierRebaseOntoAForeignEpochIsRefused", [&] {
        auto u = isolatedSchema(url, base + "_v_epoch");
        Db db(u);
        { oe::Store a(u, "node0-rec1", "rec-A"); apply(a, {cfg(64), trd(2, 160), term(3, 256)}); }
        oe::Store::rebase(u, "node1-rec1", "rec-B");
        oe::Store b(u, "node1-rec1", "rec-B");
        // A dual-genesis member: FRESH at journalSeq 1 of another epoch, inside the rebase range.
        auto foreign = cfg(96); put(foreign.bytes, EPOCH, static_cast<std::uint64_t>(G + 1), 8);
        check(error([&] { apply(b, {foreign}); }) == EPOCH_MISMATCH, "a foreign-epoch duplicate seq skipped");
        auto foreignNew = term(4, 96); put(foreignNew.bytes, EPOCH, static_cast<std::uint64_t>(G + 1), 8);
        check(error([&] { apply(b, {foreignNew}); }) == EPOCH_MISMATCH, "a foreign-epoch new seq accepted");
        check(cursor(db, "position") == 0 && cursor(db, "last_journal_seq") == 3 && cursor(db, "me_epoch") == G &&
              b.duplicateSeqs() == 0, "a foreign epoch moved the rebased checkpoint");
    });
    s3case("verifierRebaseOntoARecordingWithAHoleIsRefused", [&] {
        auto u = isolatedSchema(url, base + "_v_hole");
        Db db(u);
        { oe::Store a(u, "node0-rec1", "rec-A"); apply(a, {cfg(64), trd(2, 160), term(3, 256), outc(4, 352)}); }
        oe::Store::rebase(u, "node1-rec1", "rec-B");
        oe::Store b(u, "node1-rec1", "rec-B");
        // The restarted victim's new recording: re-journaled from P, so it starts past the cursor + 1.
        check(error([&] { apply(b, {outc(6, 96)}); }) == GAP, "a rebased recording starting past last + 1 accepted");
        // Duplicates first, then a hole right after the rebase range.
        check(error([&] { apply(b, {cfg(96), trd(2, 192), term(3, 288), outc(4, 384), outc(6, 480)}); }) == GAP,
              "a hole right after the rebase range accepted");
        check(cursor(db, "position") == 0 && cursor(db, "last_journal_seq") == 4 && cursor(db, "rebase_ceiling") == 4,
              "a refused rebased batch moved the checkpoint");
        check(db.scalar("SELECT count(*) FROM execution_journal_events WHERE source_identity='node1-rec1'") == 0,
              "a refused rebased batch left raw events");
    });
    s3case("verifierRebaseRefusesNoCursorAndTheSameSource", [&] {
        auto u = isolatedSchema(url, base + "_v_refuse");
        Db db(u);
        { oe::Store a(u, "node0-rec1", "rec-A"); }
        rejects([&] { oe::Store::rebase(u, "node1-rec1", "rec-B"); }, "rebase without a journalSeq cursor accepted");
        { oe::Store a(u, "node0-rec1", "rec-A"); apply(a, {cfg(64)}); }
        rejects([&] { oe::Store::rebase(u, "node0-rec1", "rec-A2"); }, "rebase onto the current source accepted");
        check(db.scalar("SELECT count(*) FROM execution_projector_checkpoint WHERE source_identity='node0-rec1' "
                        "AND recording_descriptor='rec-A' AND position=64 AND rebase_ceiling=0") == 1,
              "a refused rebase changed the checkpoint");
    });
    { Db db(url); db.sql("SET client_min_messages TO warning"); for (const char* s : {"_first", "_gap", "_epoch", "_regression", "_checkpoint", "_redelivery", "_rebase",
                                        "_rebase_conflict", "_v_epoch", "_v_hole", "_v_refuse"})
          db.sql("DROP SCHEMA IF EXISTS " + base + s + " CASCADE"); }
}
}
int main() {
    try {
        decodeTests();
        s3DecodeCases();
        std::cout << "PASS schema 4 decode: six Java fixtures, gate (schema 3 / schema 1 / v2 / template / block refused), "
                     "5.5 validation, release cap 48, frame bound 1112, unfragmented replay frames\n";
        // RC2 S3: the first record of a cursor is FRESH at journalSeq 1, then dense seqs.
        auto fresh = at(fixture("config", 0), 1, 128);
        auto first = at(fixture("trade", 160), 2, 160);
        std::vector<oe::Event> opening{fresh, first};
        auto cmd = at(fixture("command.first", 352), 4, 352);
        // A behaviour-1 style lost-ack copy of the outcome at a later delivery (same canonical bytes).
        auto retry = at(fixture("command.first", 512), 5, 512); put(retry.bytes, OUT_EGRESS, 320, 8);
        const char* env = std::getenv("OE_PROJECTOR_TEST_PG");
        if (!env || !*env) { std::cerr << "OE_PROJECTOR_TEST_PG required\n"; return 77; }
        const std::string url(env), schema = "projector_test_" + std::to_string(getpid());
        const auto scoped = url + " options='-c search_path=" + schema + "'";
        {
            Db db(url); db.sql("CREATE SCHEMA " + schema); db.sql("SET search_path TO " + schema);
            db.sql("CREATE TABLE orders(oms_order_id bigint PRIMARY KEY); INSERT INTO orders VALUES(101),(102);"
                "CREATE TABLE executions(execution_id bigserial PRIMARY KEY, trade_id bigint, oms_order_id bigint REFERENCES orders,"
                "user_id bigint, market_id int, side text, price bigint, quantity bigint, is_maker boolean, executed_at timestamptz)");
            std::ifstream input(OE_PROJECTOR_SCHEMA); std::string sql((std::istreambuf_iterator<char>(input)), {});
            check(!sql.empty(), "Missing schema"); db.sql(sql);
            std::ifstream ownership(OE_WRITER_SCHEMA);
            std::string fenceSql((std::istreambuf_iterator<char>(ownership)), {});
            check(!fenceSql.empty(), "Missing writer ownership migration"); db.sql(fenceSql);
            rejects([&] { oe::Store premature(scoped, "test-generation", "recording-0"); },
                    "Archive worker started while legacy still owned executions");
            db.sql("SELECT transition_execution_writer('legacy',1,'paused','test quiesce'); "
                   "SELECT transition_execution_writer('paused',2,'archive','test activation')");
            db.sql("CREATE FUNCTION fail_maker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.is_maker THEN "
                   "RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$; "
                   "CREATE TRIGGER injected BEFORE INSERT ON executions FOR EACH ROW EXECUTE FUNCTION fail_maker()");
        }
        {
            oe::Store store(scoped, "test-generation", "recording-0");
            rejects([&] { oe::Store duplicate(scoped, "test-generation", "recording-0"); }, "Concurrent writer accepted");
            rejects([&] { store.apply(opening); }, "Maker failure not surfaced");
            Db db(scoped);
            check(db.scalar("SELECT count(*) FROM executions") == 0, "Partial taker leg committed");
            check(db.scalar("SELECT count(*) FROM execution_journal_events") == 0, "Partial event committed");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 0, "Failed checkpoint advanced");
            db.sql("DROP TRIGGER injected ON executions");
        }
        // A process exits after COMMIT without acknowledging success to its caller.
        // No live PG connection is inherited across fork.
        auto pid = fork(); check(pid >= 0, "fork failed");
        if (pid == 0) {
            try { oe::Store store(scoped, "test-generation", "recording-0"); store.apply(opening); }
            catch (...) { _exit(18); }
            _exit(17);
        }
        int status{}; waitpid(pid, &status, 0); check(WIFEXITED(status) && WEXITSTATUS(status) == 17, "Commit child failed");
        {
            oe::Store restarted(scoped, "test-generation", "recording-0");
            check(restarted.position() == 160, "Committed position lost across process exit");
            restarted.apply(std::span(&first, 1)); // Duplicate delivery.
            Db db(scoped); check(db.scalar("SELECT count(*) FROM executions") == 2, "Duplicate financial history legs");
            // decodeRefusesSchema3Loudly on the writer path: no row, no checkpoint, never a skip.
            oe::Event legacy{192, schema3Trade()};
            try { restarted.apply(std::span(&legacy, 1)); check(false, "Schema 3 event applied"); }
            catch (const std::runtime_error& e) { check(e.what() == refusal(3, 1, 1), "Schema 3 refusal message"); }
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 160, "Schema 3 event advanced cursor");
            check(db.scalar("SELECT count(*) FROM execution_journal_events") == 2, "Schema 3 event recorded");
            auto different = first; put(different.bytes, TRADE_PRICE, 5, 8);
            rejects([&] { restarted.apply(std::span(&different, 1)); }, "Conflicting replay accepted");
            auto gap = trade(3, 3, 320);
            rejects([&] { restarted.apply(std::span(&gap, 1)); }, "Trade gap accepted");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 160, "Gap checkpoint advanced");
            auto end = at(fixture("terminal", 256), 3, 256); restarted.apply(std::span(&end, 1));
            check(db.scalar("SELECT count(*) FROM execution_journal_terminals") == 1, "Terminal lost");
            check(db.scalar("SELECT count(*) FROM executions") == 2, "Terminal changed execution count");
            db.sql("CREATE FUNCTION fail_outcome() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                   "RAISE EXCEPTION 'injected outcome failure'; END $$; "
                   "CREATE TRIGGER injected_outcome BEFORE INSERT ON me_command_outcomes FOR EACH ROW EXECUTE FUNCTION fail_outcome()");
            rejects([&] { restarted.apply(std::span(&cmd,1)); },"Command outcome failure not surfaced");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==256,"Failed outcome advanced cursor");
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==0,"Partial command outcome committed");
            db.sql("DROP TRIGGER injected_outcome ON me_command_outcomes");
            restarted.apply(std::span(&cmd,1)); restarted.apply(std::span(&retry,1));
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==1,"Duplicate canonical command outcome");
            check(db.scalar("SELECT count(*) FROM executions")==2,"Command outcome replayed settlement");
            check(db.scalar("SELECT count(*) FROM execution_journal_terminals")==1,"Command outcome became financial terminal");
            // A retry samples the ledger when it is journaled: a different tail is still the same outcome.
            auto resampled = at(retry, 6, 544); put(resampled.bytes, OUT_EGRESS, 300, 8);
            put(resampled.bytes, OUT_LEDGER, 7, 4); put(resampled.bytes, OUT_BEHAVIOR, 3, 4);
            restarted.apply(std::span(&resampled, 1));
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==1,"Ledger sample split a canonical outcome");
            // checkpointAdvancesOverReleaseAndConfigWithoutRows
            auto bump = at(fixture("config", 608), 8, 608); put(bump.bytes, CFG_DECISION, 3, 1); // a later BUMP
            std::vector<oe::Event> control{at(fixture("release", 576), 7, 576), bump};
            restarted.apply(control);
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==608,"Release/config did not advance cursor");
            check(db.scalar("SELECT count(*) FROM execution_journal_events WHERE template_id IN (4,5)")==3,"Release/config raw event lost"); // + the opening FRESH
            check(db.scalar("SELECT count(*) FROM executions")==2 && db.scalar("SELECT count(*) FROM execution_journal_terminals")==1 &&
                  db.scalar("SELECT count(*) FROM me_command_outcomes")==1,"Release/config projected a row");
            restarted.apply(control); // Duplicate delivery of release/config.
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==608,"Duplicate release/config moved cursor");
            // RELEASED (7) and INVALID (8) are observations: raw event only, the canonical result stays.
            std::vector<oe::Event> observed{at(observation(cmd, 7, 400, 640), 9, 640),
                                            at(observation(cmd, 8, 420, 672), 10, 672)};
            restarted.apply(observed);
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==1 &&
                  db.scalar("SELECT result FROM me_command_outcomes")==0,"Observation replaced the canonical outcome");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==672,"Observation did not advance cursor");
            auto badOutcome=at(retry,11,704); put(badOutcome.bytes,OUT_PRICE,123,8);
            rejects([&] { restarted.apply(std::span(&badOutcome,1)); },"Different canonical command accepted");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==672,"Conflict advanced cursor");
            // A behaviour-1 DUPLICATE journals the retry frame's timeInForce (Engine.java:74-86,
            // DurableCommandTest.retryWithDifferentTifIsStillADuplicateInBehaviour1): same outcome.
            auto otherTif=at(retry,11,704); put(otherTif.bytes,OUT_TIF,1,1);
            restarted.apply(std::span(&otherTif,1));
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==704,"Retry with another timeInForce halted");
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==1 &&
                  db.scalar("SELECT count(*) FROM me_command_outcomes WHERE canonical_payload=decode('" +
                            oe::hex(std::span(cmd.bytes).subspan(oe::OUTCOME_CANONICAL_BEGIN,
                                    oe::OUTCOME_CANONICAL_END-oe::OUTCOME_CANONICAL_BEGIN)) + "','hex')")==1,
                  "Retry with another timeInForce replaced the canonical outcome");
            restarted.observe(900);
            check(db.scalar("SELECT observed_target FROM execution_projector_checkpoint") == 900, "Observed target not published");
            check(db.scalar("SELECT CASE WHEN NOW()-observed_at < interval '3 seconds' THEN 1 ELSE 0 END "
                            "FROM execution_projector_checkpoint") == 1, "Observation not stamped with database time");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 704, "Observation moved the cursor");
            rejects([&] { restarted.observe(680); }, "Observed target regressed below the durable cursor");
            db.sql("SELECT transition_execution_writer('archive',3,'paused','test pause')");
            rejects([&] { restarted.probe(); }, "Idle worker ignored revoked ownership");
            rejects([&] { restarted.observe(1000); }, "Revoked worker still published freshness");
            auto lateTerminal = fixture("terminal", 768);
            rejects([&] { restarted.apply(std::span(&lateTerminal, 1)); }, "Terminal batch ignored revoked ownership");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 704, "Revoked writer advanced checkpoint");
            db.sql("SELECT transition_execution_writer('paused',4,'archive','test reactivate')");
            rejects([&] { restarted.probe(); }, "Stale epoch resumed after reactivation");
        }
        rejects([&] { oe::Store wrong(scoped, "different-generation", "recording-0"); }, "Generation fence failed");
        rejects([&] { oe::Store wrong(scoped, "test-generation", "different-recording"); }, "Recording fence failed");
        // Verifier additions (RC2 S1 review): the canonical outcome window [32,140) byte by byte,
        // and schema 1 template 28 on the writer path. Runs on the reactivated epoch-4 owner.
        {
            oe::Store again(scoped, "test-generation", "recording-0");
            Db db(scoped);
            const auto cursor = again.position();
            check(cursor == 704, "Verifier: unexpected resume cursor");
            // decodeRefusesSchema1Template28 on the writer path: exact message, no row, no cursor move.
            oe::Event legacyOutcome{cursor + 32, schema1Outcome()};
            try { again.apply(std::span(&legacyOutcome, 1)); check(false, "Schema 1 outcome applied"); }
            catch (const std::runtime_error& e) { check(e.what() == refusal(1, 11, 28), "Schema 1 refusal message"); }
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == cursor, "Schema 1 event advanced cursor");
            // Every byte after the command id up to timeInForce, [48,140), is part of the retry match:
            // a one-byte change is refused (decode or conflict) and never moves the cursor.
            // ([32,48) is the command id itself: a change there is a different command, tested below.)
            std::int64_t seq = 11; // the cursor at 704: otherTif above
            for (int p = OUT_LOW + 8; p < OUT_TIF; ++p) {
                auto changed = at(retry, seq + 1, cursor + 32); changed.bytes.at(p) ^= 0x02;
                bool refused = false; try { again.apply(std::span(&changed, 1)); } catch (const std::exception&) { refused = true; }
                if (!refused) std::cerr << "accepted canonical byte " << p << "\n";
                check(refused, "Verifier: changed canonical byte accepted as a retry");
            }
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == cursor, "Verifier: refused retry moved cursor");
            // Every byte of the delivery head and of the echo + ledger sample tail [140,161) may differ.
            // RC2 S3: journalSeq [8,16) and meEpoch [16,24) are the producer cursor now (dense, one
            // epoch; the S3 cases below), so only egressSeq [24,32) of the head varies here.
            auto next = cursor;
            for (int p = OUT_EGRESS; p < static_cast<int>(retry.bytes.size()); ++p) {
                if (p == oe::OUTCOME_CANONICAL_BEGIN) p = oe::OUTCOME_CANONICAL_END;
                // expectedOldRemaining is outside the window, but decode requires 0 on a CREATE.
                if (p == OUT_EOR) p = OUT_LEDGER;
                // behaviorVersion is 2 in the S3 fixtures: ^0x02 would make it 0 (refused), so ^0x04.
                next += 32; auto sampled = at(retry, ++seq, next); sampled.bytes.at(p) ^= (p == OUT_BEHAVIOR ? 0x04 : 0x02);
                again.apply(std::span(&sampled, 1));
            }
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == next, "Verifier: head/tail retry did not advance");
            check(db.scalar("SELECT count(*) FROM me_command_outcomes") == 1 &&
                  db.scalar("SELECT result FROM me_command_outcomes") == 0, "Verifier: head/tail retry split the canonical outcome");
            // A change inside the command id [32,48) is a different command: its own canonical row.
            for (int p = oe::OUTCOME_CANONICAL_BEGIN; p < OUT_LOW + 8; ++p) {
                next += 32; auto other = at(retry, ++seq, next); other.bytes.at(p) ^= 0x02;
                again.apply(std::span(&other, 1));
            }
            check(db.scalar("SELECT count(*) FROM me_command_outcomes") == 17, "Verifier: distinct command ids not kept apart");
        }
        { Db db(url); db.sql("DROP SCHEMA " + schema + " CASCADE"); }
        std::cout << "PASS schema 4 projection: atomic legs/checkpoint, process restart, replay, conflict, gap, terminal, source and writer fences\n";
        s3StoreCases(url);
        if (s3Failures) { std::cerr << s3Failures << " RC2 S3 case(s) failed\n"; return 1; }
        std::cout << "PASS RC2 S3 projector: journalSeq >= 1 dense, one epoch, FRESH first, rebase-source\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
