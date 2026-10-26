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
oe::Event trade(std::int64_t id, std::int64_t position) {
    auto e = fixture("trade", position); put(e.bytes, TRADE_ID, id, 8); return e;
}
oe::Event observation(const oe::Event& outcome, int result, std::int64_t egress, std::int64_t position) {
    auto e = outcome; e.position = position;
    put(e.bytes, OUT_EGRESS, egress, 8); put(e.bytes, OUT_ORDER, 0, 8);
    put(e.bytes, OUT_STATUS, static_cast<std::uint32_t>(-1), 4); put(e.bytes, OUT_RESULT, result, 4);
    return e;
}
oe::Event release(std::size_t entries, std::int64_t position) {
    oe::Event e{position, std::vector<std::uint8_t>(REL_ENTRIES + 22 * entries)}; auto& b = e.bytes;
    header(b, 44, 4, 4, 1); put(b, 24, 640, 8); put(b, 32, 7, 8); put(b, 40, 1, 4); put(b, 44, 100000, 4);
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
    auto t = oe::decode(fixture("trade", 0).bytes);
    check(t.type == 1 && t.journalSeq == 0 && t.meEpoch == 0 && t.egressSeq == 10000 && t.trade == 1 &&
          t.market == 1 && t.taker == 91 && t.takerUser == 11 && t.maker == 92 && t.makerUser == 12 &&
          t.price == 12345678901234 && t.quantity == 123456789 && t.sideOrStatus == 1 &&
          t.timestamp == 1700000000123 && t.takerOms == 101 && t.makerOms == 102, "Java trade fixture mismatch");
    auto e = oe::decode(fixture("terminal", 0).bytes);
    check(e.type == 2 && e.egressSeq == 10000 && e.taker == 91 && e.takerUser == 11 && e.market == 1 &&
          e.sideOrStatus == 2 && e.timestamp == 1700000000123 && e.takerOms == 101 && e.rejectReason == 0,
          "Java terminal fixture mismatch");
    auto o = oe::decode(fixture("command.first", 0).bytes);
    check(o.type == 3 && o.egressSeq == 128 && o.commandHigh == 17 && o.commandLow == 1 && o.takerUser == 100 &&
          o.takerOms == 9001 && o.oldOrder == 0 && o.price == 5900000000000 && o.quantity == 100000000 &&
          o.budget == 0 && o.market == 1 && o.kind == 0 && o.orderType == 0 && o.orderSide == 0 &&
          o.appliedPosition == 128 && o.timestamp == 1000 && o.taker == 1 && o.status == 0 && o.reason == 0 &&
          !o.oldCancelled && o.result == 0 && o.timeInForce == 0 && o.expectedOldRemaining == 0 &&
          o.ledgerSize == 1 && o.ledgerCapacity == 100000 && o.behaviorVersion == 1, "Java command fixture mismatch");
    auto r = oe::decode(fixture("command.retry", 0).bytes);
    check(r.type == 3 && r.egressSeq == 256 && r.appliedPosition == 128 && r.commandLow == 1, "Java retry fixture mismatch");
    auto l = oe::decode(fixture("release", 0).bytes);
    check(l.type == 4 && l.egressSeq == 640 && l.releaseSeq == 7 && l.ledgerSize == 1 && l.ledgerCapacity == 100000 &&
          l.tombstoneRanges == 1 && l.releases.size() == 2 &&
          l.releases[0].commandHigh == 17 && l.releases[0].idLowFloor == 2 && l.releases[0].scope == 0 &&
          l.releases[0].result == 0 && l.releases[0].removed == 1 &&
          l.releases[1].commandHigh == 17 && l.releases[1].idLowFloor == 5 && l.releases[1].scope == 1 &&
          l.releases[1].result == 1 && l.releases[1].removed == 0, "Java release fixture mismatch");
    auto c = oe::decode(fixture("config", 0).bytes);
    check(c.type == 5 && c.egressSeq == 64 && c.configVersion == 1 && c.behaviorVersion == 1 &&
          c.ledgerCapacity == 100000 && c.maxWireVersion == 12 && c.decision == 0, "Java config fixture mismatch");
    for (const char* name : {"trade", "terminal", "command.first", "command.retry", "release", "config"}) {
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
        check(oe::decode(seq).journalSeq == 5 && oe::decode(seq).meEpoch == 3, "S1 journalSeq/meEpoch >= 0 refused");
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
}
int main() {
    try {
        decodeTests();
        std::cout << "PASS schema 4 decode: six Java fixtures, gate (schema 3 / schema 1 / v2 / template / block refused), "
                     "5.5 validation, release cap 48, frame bound 1112, unfragmented replay frames\n";
        auto first = fixture("trade", 160);
        auto cmd = fixture("command.first", 352), retry = fixture("command.retry", 512);
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
            rejects([&] { store.apply(std::span(&first, 1)); }, "Maker failure not surfaced");
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
            try { oe::Store store(scoped, "test-generation", "recording-0"); store.apply(std::span(&first, 1)); }
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
            check(db.scalar("SELECT count(*) FROM execution_journal_events") == 1, "Schema 3 event recorded");
            auto different = first; put(different.bytes, TRADE_PRICE, 5, 8);
            rejects([&] { restarted.apply(std::span(&different, 1)); }, "Conflicting replay accepted");
            auto gap = trade(3, 320);
            rejects([&] { restarted.apply(std::span(&gap, 1)); }, "Trade gap accepted");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 160, "Gap checkpoint advanced");
            auto end = fixture("terminal", 256); restarted.apply(std::span(&end, 1));
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
            auto resampled = retry; resampled.position = 544; put(resampled.bytes, OUT_EGRESS, 300, 8);
            put(resampled.bytes, OUT_LEDGER, 7, 4); put(resampled.bytes, OUT_BEHAVIOR, 2, 4); put(resampled.bytes, 8, 9, 8);
            restarted.apply(std::span(&resampled, 1));
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==1,"Ledger sample split a canonical outcome");
            // checkpointAdvancesOverReleaseAndConfigWithoutRows
            std::vector<oe::Event> control{fixture("release", 576), fixture("config", 608)};
            restarted.apply(control);
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==608,"Release/config did not advance cursor");
            check(db.scalar("SELECT count(*) FROM execution_journal_events WHERE template_id IN (4,5)")==2,"Release/config raw event lost");
            check(db.scalar("SELECT count(*) FROM executions")==2 && db.scalar("SELECT count(*) FROM execution_journal_terminals")==1 &&
                  db.scalar("SELECT count(*) FROM me_command_outcomes")==1,"Release/config projected a row");
            restarted.apply(control); // Duplicate delivery of release/config.
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==608,"Duplicate release/config moved cursor");
            // RELEASED (7) and INVALID (8) are observations: raw event only, the canonical result stays.
            std::vector<oe::Event> observed{observation(cmd, 7, 400, 640), observation(cmd, 8, 420, 672)};
            restarted.apply(observed);
            check(db.scalar("SELECT count(*) FROM me_command_outcomes")==1 &&
                  db.scalar("SELECT result FROM me_command_outcomes")==0,"Observation replaced the canonical outcome");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==672,"Observation did not advance cursor");
            auto badOutcome=retry; badOutcome.position=704; put(badOutcome.bytes,OUT_PRICE,123,8);
            rejects([&] { restarted.apply(std::span(&badOutcome,1)); },"Different canonical command accepted");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint")==672,"Conflict advanced cursor");
            // A behaviour-1 DUPLICATE journals the retry frame's timeInForce (Engine.java:74-86,
            // DurableCommandTest.retryWithDifferentTifIsStillADuplicateInBehaviour1): same outcome.
            auto otherTif=retry; otherTif.position=704; put(otherTif.bytes,OUT_TIF,1,1);
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
            for (int p = OUT_LOW + 8; p < OUT_TIF; ++p) {
                auto changed = retry; changed.position = cursor + 32; changed.bytes.at(p) ^= 0x02;
                bool refused = false; try { again.apply(std::span(&changed, 1)); } catch (const std::exception&) { refused = true; }
                if (!refused) std::cerr << "accepted canonical byte " << p << "\n";
                check(refused, "Verifier: changed canonical byte accepted as a retry");
            }
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == cursor, "Verifier: refused retry moved cursor");
            // Every byte of the delivery head [8,32) and of the echo + ledger sample tail [140,161) may differ.
            auto next = cursor;
            for (int p = 8; p < static_cast<int>(retry.bytes.size()); ++p) {
                if (p == oe::OUTCOME_CANONICAL_BEGIN) p = oe::OUTCOME_CANONICAL_END;
                // expectedOldRemaining is outside the window, but decode requires 0 on a CREATE.
                if (p == OUT_EOR) p = OUT_LEDGER;
                auto sampled = retry; next += 32; sampled.position = next; sampled.bytes.at(p) ^= 0x02;
                again.apply(std::span(&sampled, 1));
            }
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == next, "Verifier: head/tail retry did not advance");
            check(db.scalar("SELECT count(*) FROM me_command_outcomes") == 1 &&
                  db.scalar("SELECT result FROM me_command_outcomes") == 0, "Verifier: head/tail retry split the canonical outcome");
            // A change inside the command id [32,48) is a different command: its own canonical row.
            for (int p = oe::OUTCOME_CANONICAL_BEGIN; p < OUT_LOW + 8; ++p) {
                auto other = retry; next += 32; other.position = next; other.bytes.at(p) ^= 0x02;
                again.apply(std::span(&other, 1));
            }
            check(db.scalar("SELECT count(*) FROM me_command_outcomes") == 17, "Verifier: distinct command ids not kept apart");
        }
        { Db db(url); db.sql("DROP SCHEMA " + schema + " CASCADE"); }
        std::cout << "PASS schema 4 projection: atomic legs/checkpoint, process restart, replay, conflict, gap, terminal, source and writer fences\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
