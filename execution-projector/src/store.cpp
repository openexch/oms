// SPDX-License-Identifier: Apache-2.0
#include "store.hpp"
#include <utility>

namespace oe {
namespace {
std::string n(std::int64_t v) { return std::to_string(v); }
Result exec(PGconn* db, const char* sql, const std::vector<std::string>& values) {
    std::vector<const char*> args; args.reserve(values.size());
    for (const auto& s : values) args.push_back(s.c_str());
    Result r(PQexecParams(db, sql, static_cast<int>(args.size()), nullptr, args.data(), nullptr, nullptr, 0));
    if (!r || (PQresultStatus(r.get()) != PGRES_COMMAND_OK && PQresultStatus(r.get()) != PGRES_TUPLES_OK)) {
        // Do not print connection strings or row values. SQLSTATE suffices for diagnosis.
        const char* state = r ? PQresultErrorField(r.get(), PG_DIAG_SQLSTATE) : nullptr;
        throw std::runtime_error(std::string("Projection SQL failed; SQLSTATE=") + (state ? state : "connection"));
    }
    return r;
}
// The archive writer's connection: bounded timeouts, the advisory lock that excludes a second
// worker (and a rebase while a worker runs), and the archive ownership epoch pinned in the session.
PGconn* openWriter(const std::string& conninfo, std::int64_t& epoch) {
    const char* keys[] = {"dbname", "connect_timeout", "tcp_user_timeout", "keepalives_idle", "keepalives_interval", "keepalives_count", nullptr};
    const char* values[] = {conninfo.c_str(), "5", "5000", "5", "1", "3", nullptr};
    PGconn* db = PQconnectdbParams(keys, values, 1);
    try {
        if (!db || PQstatus(db) != CONNECTION_OK) throw std::runtime_error("Projection database unavailable");
        exec(db, "SET statement_timeout='5s'", {}); exec(db, "SET lock_timeout='2s'", {});
        auto lock = exec(db, "SELECT pg_try_advisory_lock(1751474543, 1702390115)", {});
        if (std::string(PQgetvalue(lock.get(), 0, 0)) != "t") throw std::runtime_error("Execution writer already active");
        auto owner = exec(db, "SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' AND owner='archive'", {});
        if (PQntuples(owner.get()) != 1) throw std::runtime_error("Execution writer ownership is not archive");
        epoch = std::stoll(PQgetvalue(owner.get(), 0, 0));
        exec(db, "SELECT set_config('oe.execution_writer','archive',false), "
                 "set_config('oe.execution_writer_epoch',$1,false)", {n(epoch)});
    } catch (...) { PQfinish(db); throw; }
    return db;
}
}
Result Store::query(const char* sql, const std::vector<std::string>& values) { return exec(db_, sql, values); }
Store::Store(const std::string& conninfo, std::string source, std::string descriptor) : source_(std::move(source)) {
    if (source_.empty() || descriptor.empty()) throw std::runtime_error("Source identity and descriptor required");
    db_ = openWriter(conninfo, writerEpoch_);
    try {
        // Constructor checkpoint creation also needs the ownership row lock.
        query("BEGIN"); checkOwnership(true);
        query("INSERT INTO execution_projector_checkpoint(consumer,source_identity,recording_descriptor,position) "
              "VALUES ('executions',$1,$2,0) ON CONFLICT(consumer) DO NOTHING", {source_, descriptor});
        auto r = query("SELECT source_identity,recording_descriptor,position,last_trade_id,last_journal_seq,me_epoch,"
                       "rebase_ceiling FROM execution_projector_checkpoint WHERE consumer='executions'");
        if (PQntuples(r.get()) != 1 || source_ != PQgetvalue(r.get(), 0, 0) || descriptor != PQgetvalue(r.get(), 0, 1))
            // The descriptor is the archive's recording identity (ids and channel), never a secret:
            // the operator needs it for --rebase-source.
            throw std::runtime_error("Source/recording fence mismatch; explicit recovery required (this run: source="
                    + source_ + " descriptor=" + descriptor + "; see --rebase-source)");
        position_ = std::stoll(PQgetvalue(r.get(), 0, 2)); trade_ = std::stoll(PQgetvalue(r.get(), 0, 3));
        seq_ = std::stoll(PQgetvalue(r.get(), 0, 4)); epoch_ = std::stoll(PQgetvalue(r.get(), 0, 5));
        ceiling_ = std::stoll(PQgetvalue(r.get(), 0, 6));
        query("COMMIT");
    } catch (...) { PQfinish(db_); db_ = nullptr; throw; }
}
Store::~Store() { if (db_) PQfinish(db_); }

void Store::rebase(const std::string& conninfo, const std::string& source, const std::string& descriptor) {
    if (source.empty() || descriptor.empty()) throw std::runtime_error("Source identity and descriptor required");
    std::int64_t epoch{};
    // openWriter takes the worker's advisory lock: a running worker makes the rebase fail here.
    PGconn* db = openWriter(conninfo, epoch);
    try {
        exec(db, "BEGIN", {});
        auto owner = exec(db, "SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' "
                              "AND owner='archive' AND epoch=$1 FOR SHARE", {n(epoch)});
        if (PQntuples(owner.get()) != 1) throw std::runtime_error("Execution writer owner/epoch changed; recovery required");
        auto r = exec(db, "SELECT source_identity,last_journal_seq FROM execution_projector_checkpoint "
                          "WHERE consumer='executions' FOR UPDATE", {});
        if (PQntuples(r.get()) != 1) throw std::runtime_error("Rebase refused: no checkpoint to rebase");
        const std::int64_t last = std::stoll(PQgetvalue(r.get(), 0, 1));
        if (last < 1) throw std::runtime_error("Rebase refused: the checkpoint has no journalSeq cursor yet");
        // execution_journal_events is keyed by (source_identity, position) and the new recording's
        // positions restart at 0: the new source identity must be one never projected before.
        auto used = exec(db, "SELECT 1 FROM execution_journal_events WHERE source_identity=$1 LIMIT 1", {source});
        if (source == PQgetvalue(r.get(), 0, 0) || PQntuples(used.get()) != 0)
            throw std::runtime_error("Rebase refused: source identity " + source + " already has journal rows");
        exec(db, "UPDATE execution_projector_checkpoint SET source_identity=$1,recording_descriptor=$2,position=0,"
                 "rebase_ceiling=last_journal_seq,observed_target=0,updated_at=NOW() WHERE consumer='executions'",
             {source, descriptor});
        exec(db, "COMMIT", {});
    } catch (...) {
        auto error = std::current_exception();
        try { exec(db, "ROLLBACK", {}); } catch (...) {}
        PQfinish(db);
        std::rethrow_exception(error);
    }
    PQfinish(db);
}

void Store::checkOwnership(bool lock) {
    auto owner = query(lock
            ? "SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' AND owner='archive' AND epoch=$1 FOR SHARE"
            : "SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' AND owner='archive' AND epoch=$1",
            {n(writerEpoch_)});
    if (PQntuples(owner.get()) != 1) throw std::runtime_error("Execution writer owner/epoch changed; recovery required");
}
void Store::probe() { checkOwnership(false); }
void Store::observe(std::int64_t recordedTarget) {
    // Consumers on other hosts read this with the database clock to know this worker is live and
    // how far the recording extends beyond the committed cursor.
    if (recordedTarget < position_) throw std::runtime_error("Observed recording end is behind the durable cursor");
    query("BEGIN");
    try {
        checkOwnership(true);
        auto r = query("UPDATE execution_projector_checkpoint SET observed_target=$1,observed_at=NOW() "
                       "WHERE consumer='executions' AND position=$2", {n(recordedTarget), n(position_)});
        if (std::string(PQcmdTuples(r.get())) != "1") throw std::runtime_error("Checkpoint changed externally");
        query("COMMIT");
    } catch (...) {
        auto error = std::current_exception();
        try { query("ROLLBACK"); } catch (...) {}
        std::rethrow_exception(error);
    }
}

void Store::leg(const Journal& j, bool maker) {
    const bool buy = maker ? !j.sideOrStatus : j.sideOrStatus;
    auto r = query(
        "INSERT INTO executions(trade_id,oms_order_id,user_id,market_id,side,price,quantity,is_maker,executed_at) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,TIMESTAMPTZ 'epoch' + $9::bigint * INTERVAL '1 millisecond') "
        "ON CONFLICT(trade_id,is_maker) DO UPDATE SET trade_id=EXCLUDED.trade_id "
        "WHERE executions.oms_order_id=EXCLUDED.oms_order_id AND executions.user_id=EXCLUDED.user_id "
        "AND executions.market_id=EXCLUDED.market_id AND executions.side=EXCLUDED.side "
        "AND executions.price=EXCLUDED.price AND executions.quantity=EXCLUDED.quantity RETURNING trade_id",
        {n(j.trade), n(maker ? j.makerOms : j.takerOms), n(maker ? j.makerUser : j.takerUser), n(j.market),
         buy ? "BUY" : "SELL", n(j.price), n(j.quantity), maker ? "true" : "false", n(j.timestamp)});
    // Legacy rows use OMS arrival time. Preserve it, but require exact economic equality.
    if (PQntuples(r.get()) != 1) throw std::runtime_error("Conflicting execution payload");
}

void Store::apply(std::span<const Event> batch) {
    if (batch.empty()) return;
    if (batch.size() > 256) throw std::runtime_error("Projection batch exceeds bound");
    query("BEGIN");
    auto nextPosition = position_, nextTrade = trade_, nextSeq = seq_, nextEpoch = epoch_, nextCeiling = ceiling_;
    auto duplicates = duplicates_;
    try {
        checkOwnership(true);
        auto fence = query("SELECT position FROM execution_projector_checkpoint WHERE consumer='executions' FOR UPDATE");
        if (std::stoll(PQgetvalue(fence.get(), 0, 0)) != position_) throw std::runtime_error("Checkpoint changed externally");
        for (const auto& event : batch) {
            auto j = decode(event.bytes);
            if (event.position <= 0 || event.position % 32 != 0) throw std::runtime_error("Invalid replay position");
            auto payload = hex(event.bytes);
            if (event.position <= nextPosition) {
                auto old = query("SELECT 1 FROM execution_journal_events WHERE source_identity=$1 AND position=$2 "
                                 "AND payload=decode($3,'hex')", {source_, n(event.position), payload});
                if (PQntuples(old.get()) != 1) throw std::runtime_error("Replay payload/position conflict");
                continue;
            }
            // RC2 S3 (contract 10.4): past the position cursor the producer cursor decides; first
            // matching rule wins. Every refusal leaves the whole batch and its checkpoint unwritten.
            if (nextSeq == 0) {
                if (j.journalSeq != 1 || j.type != RECORD_CONFIG || j.decision != 0 /* FRESH */)
                    throw std::runtime_error("First journal record must be EngineConfig FRESH at journalSeq 1; "
                                             "checkpoint not advanced");
                nextEpoch = j.meEpoch;
            } else if (j.meEpoch != nextEpoch) {
                throw std::runtime_error("Journal epoch mismatch; checkpoint not advanced");
            } else if (j.journalSeq <= nextSeq) {
                // Only a rebased source replays seqs already projected; on one producer recording a
                // seq at or below the cursor at a new position cannot occur.
                if (j.journalSeq > nextCeiling) throw std::runtime_error("Journal sequence regression; checkpoint not advanced");
                if (j.type == RECORD_TRADE) {
                    auto old = query("SELECT 1 FROM execution_journal_trades WHERE trade_id=$1 AND payload=decode($2,'hex')",
                                     {n(j.trade), payload});
                    if (PQntuples(old.get()) != 1) throw std::runtime_error("Conflicting repeated trade");
                }
                ++duplicates; nextPosition = event.position;
                continue; // already projected: no raw event, no projection row
            } else if (j.journalSeq != nextSeq + 1) {
                throw std::runtime_error("Journal sequence gap; checkpoint not advanced");
            }
            nextSeq = j.journalSeq; nextCeiling = 0;
            query("INSERT INTO execution_journal_events(source_identity,position,template_id,payload) "
                  "VALUES($1,$2,$3,decode($4,'hex'))", {source_, n(event.position), n(j.type), payload});
            if (j.type == RECORD_TRADE) {
                if (j.trade > nextTrade && j.trade - nextTrade != 1) throw std::runtime_error("Trade gap; checkpoint not advanced");
                if (j.trade <= nextTrade) {
                    auto old = query("SELECT 1 FROM execution_journal_trades WHERE trade_id=$1 AND payload=decode($2,'hex')",
                                     {n(j.trade), payload});
                    if (PQntuples(old.get()) != 1) throw std::runtime_error("Conflicting repeated trade");
                } else {
                    query("INSERT INTO execution_journal_trades(trade_id,payload) VALUES($1,decode($2,'hex'))", {n(j.trade), payload});
                    leg(j, false); leg(j, true); nextTrade = j.trade;
                }
            } else if (j.type == RECORD_OUTCOME) {
                // CONFLICT/CAPACITY/RELEASED/INVALID are observations, not a new canonical outcome
                // for the id. Preserve their complete raw event without overwriting the original result.
                if (j.result != 2 && j.result != 3 && j.result != 7 && j.result != 8) {
                    // Omit header, delivery head, intent echo and ledger sample (journal.hpp).
                    auto canonical = hex(std::span(event.bytes).subspan(OUTCOME_CANONICAL_BEGIN,
                            OUTCOME_CANONICAL_END - OUTCOME_CANONICAL_BEGIN));
                    auto saved = query("INSERT INTO me_command_outcomes(command_id_high,command_id_low,user_id,oms_order_id,"
                        "market_id,command_kind,old_order_id,order_id,old_cancelled,status,reason,result,applied_position,"
                        "event_time_ms,source_identity,first_position,canonical_payload) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,decode($17,'hex')) "
                        "ON CONFLICT(command_id_high,command_id_low) DO UPDATE SET command_id_high=EXCLUDED.command_id_high "
                        "WHERE me_command_outcomes.canonical_payload=EXCLUDED.canonical_payload RETURNING command_id_high",
                        {n(j.commandHigh),n(j.commandLow),n(j.takerUser),n(j.takerOms),n(j.market),n(j.kind),n(j.oldOrder),
                         n(j.taker),j.oldCancelled?"true":"false",n(j.status),n(j.reason),n(j.result),n(j.appliedPosition),
                         n(j.timestamp),source_,n(event.position),canonical});
                    if (PQntuples(saved.get())!=1) throw std::runtime_error("Conflicting durable command outcome");
                }
            } else if (j.type == RECORD_RELEASE || j.type == RECORD_CONFIG) {
                // Decoded and validated; kept only as the raw event above. No projection row in S1.
            } else {
                // Preserve authoritative per-ME-leg terminals. An iceberg slice terminal
                // cannot terminalize its OMS parent or authorize a financial release here.
                query("INSERT INTO execution_journal_terminals(source_identity,position,oms_order_id,cluster_order_id,"
                      "user_id,market_id,status,event_time_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                      {source_, n(event.position), n(j.takerOms), n(j.taker), n(j.takerUser), n(j.market),
                       n(j.sideOrStatus), n(j.timestamp)});
            }
            nextPosition = event.position;
        }
        query("UPDATE execution_projector_checkpoint SET position=$1,last_trade_id=$2,last_journal_seq=$3,me_epoch=$4,"
              "rebase_ceiling=$5,updated_at=NOW() WHERE consumer='executions'",
              {n(nextPosition), n(nextTrade), n(nextSeq), n(nextEpoch), n(nextCeiling)});
        query("COMMIT");
        position_ = nextPosition; trade_ = nextTrade; seq_ = nextSeq; epoch_ = nextEpoch; ceiling_ = nextCeiling;
        duplicates_ = duplicates;
    } catch (...) {
        // Unknown COMMIT outcome terminates this worker. On restart PostgreSQL's
        // committed cursor is authoritative; never re-use speculative in-memory state.
        auto error = std::current_exception();
        try { query("ROLLBACK"); } catch (...) {}
        std::rethrow_exception(error);
    }
}
}
