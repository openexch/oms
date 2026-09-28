// SPDX-License-Identifier: Apache-2.0
#include "store.hpp"
#include <cstdlib>
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
oe::Event trade(std::int64_t id = 1, std::int64_t position = 160) {
    oe::Event e{position, std::vector<std::uint8_t>(101)};
    auto& b = e.bytes;
    put(b, 0, 93, 2); put(b, 2, 1, 2); put(b, 4, 3, 2); put(b, 6, 1, 2);
    put(b, 8, 10000, 8); put(b, 16, id, 8); put(b, 24, 1, 4);
    put(b, 28, 91, 8); put(b, 36, 11, 8); put(b, 44, 92, 8); put(b, 52, 12, 8);
    put(b, 60, 12345678901234, 8); put(b, 68, 123456789, 8); put(b, 76, 1, 1);
    put(b, 77, 1700000000123, 8); put(b, 85, 101, 8); put(b, 93, 102, 8);
    return e;
}
oe::Event terminal() {
    oe::Event e{256, std::vector<std::uint8_t>(53)}; auto& b = e.bytes;
    put(b, 0, 45, 2); put(b, 2, 2, 2); put(b, 4, 3, 2); put(b, 6, 1, 2);
    put(b, 8, 10000, 8); put(b, 16, 91, 8); put(b, 24, 11, 8); put(b, 32, 1, 4);
    put(b, 36, 2, 1); put(b, 37, 1700000000123, 8); put(b, 45, 101, 8);
    return e;
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
        auto first = trade(); auto decoded = oe::decode(first.bytes);
        check(decoded.price == 12345678901234 && decoded.quantity == 123456789 && decoded.takerOms == 101, "Exact decoding");
        for (std::size_t size = 0; size < first.bytes.size(); ++size)
            rejects([&] { oe::decode(std::span(first.bytes).first(size)); }, "Truncated payload accepted");
        for (int offset : {0, 2, 4, 6, 76}) {
            auto bad = first; bad.bytes[offset] = 99;
            rejects([&] { oe::decode(bad.bytes); }, "Invalid wire header/enum accepted");
        }
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
            auto different = first; put(different.bytes, 60, 5, 8);
            rejects([&] { restarted.apply(std::span(&different, 1)); }, "Conflicting replay accepted");
            auto gap = trade(3, 320);
            rejects([&] { restarted.apply(std::span(&gap, 1)); }, "Trade gap accepted");
            check(db.scalar("SELECT position FROM execution_projector_checkpoint") == 160, "Gap checkpoint advanced");
            auto end = terminal(); restarted.apply(std::span(&end, 1));
            check(db.scalar("SELECT count(*) FROM execution_journal_terminals") == 1, "Terminal lost");
            check(db.scalar("SELECT count(*) FROM executions") == 2, "Terminal changed execution count");
        }
        rejects([&] { oe::Store wrong(scoped, "different-generation", "recording-0"); }, "Generation fence failed");
        rejects([&] { oe::Store wrong(scoped, "test-generation", "different-recording"); }, "Recording fence failed");
        { Db db(url); db.sql("DROP SCHEMA " + schema + " CASCADE"); }
        std::cout << "PASS decoder bounds, atomic legs/checkpoint, process restart, replay, conflict, gap, terminal, source and writer fences\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
