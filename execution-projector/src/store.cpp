// SPDX-License-Identifier: Apache-2.0
#include "store.hpp"
#include <utility>

namespace oe {
namespace { std::string n(std::int64_t v) { return std::to_string(v); } }
Result Store::query(const char* sql, const std::vector<std::string>& values) {
    std::vector<const char*> args; args.reserve(values.size());
    for (const auto& s : values) args.push_back(s.c_str());
    Result r(PQexecParams(db_, sql, static_cast<int>(args.size()), nullptr, args.data(), nullptr, nullptr, 0));
    if (!r || (PQresultStatus(r.get()) != PGRES_COMMAND_OK && PQresultStatus(r.get()) != PGRES_TUPLES_OK)) {
        // Do not print connection strings or row values. SQLSTATE suffices for diagnosis.
        const char* state = r ? PQresultErrorField(r.get(), PG_DIAG_SQLSTATE) : nullptr;
        throw std::runtime_error(std::string("Projection SQL failed; SQLSTATE=") + (state ? state : "connection"));
    }
    return r;
}
Store::Store(const std::string& conninfo, std::string source, std::string descriptor) : source_(std::move(source)) {
    if (source_.empty() || descriptor.empty()) throw std::runtime_error("Source identity and descriptor required");
    const char* keys[] = {"dbname", "connect_timeout", "tcp_user_timeout", "keepalives_idle", "keepalives_interval", "keepalives_count", nullptr};
    const char* values[] = {conninfo.c_str(), "5", "5000", "5", "1", "3", nullptr};
    db_ = PQconnectdbParams(keys, values, 1);
    try {
        if (!db_ || PQstatus(db_) != CONNECTION_OK) throw std::runtime_error("Projection database unavailable");
        query("SET statement_timeout='5s'"); query("SET lock_timeout='2s'");
        auto lock = query("SELECT pg_try_advisory_lock(1751474543, 1702390115)");
        if (std::string(PQgetvalue(lock.get(), 0, 0)) != "t") throw std::runtime_error("Execution writer already active");
        auto owner = query("SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' AND owner='archive'");
        if (PQntuples(owner.get()) != 1) throw std::runtime_error("Execution writer ownership is not archive");
        writerEpoch_ = std::stoll(PQgetvalue(owner.get(), 0, 0));
        query("SELECT set_config('oe.execution_writer','archive',false), "
              "set_config('oe.execution_writer_epoch',$1,false)", {n(writerEpoch_)});
        // Constructor checkpoint creation also needs the ownership row lock.
        query("BEGIN"); checkOwnership(true);
        query("INSERT INTO execution_projector_checkpoint(consumer,source_identity,recording_descriptor,position) "
              "VALUES ('executions',$1,$2,0) ON CONFLICT(consumer) DO NOTHING", {source_, descriptor});
        auto r = query("SELECT source_identity,recording_descriptor,position,last_trade_id "
                       "FROM execution_projector_checkpoint WHERE consumer='executions'");
        if (PQntuples(r.get()) != 1 || source_ != PQgetvalue(r.get(), 0, 0) || descriptor != PQgetvalue(r.get(), 0, 1))
            throw std::runtime_error("Source/recording fence mismatch; explicit recovery required");
        position_ = std::stoll(PQgetvalue(r.get(), 0, 2)); trade_ = std::stoll(PQgetvalue(r.get(), 0, 3));
        query("COMMIT");
    } catch (...) { PQfinish(db_); db_ = nullptr; throw; }
}
Store::~Store() { if (db_) PQfinish(db_); }

void Store::checkOwnership(bool lock) {
    auto owner = query(lock
            ? "SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' AND owner='archive' AND epoch=$1 FOR SHARE"
            : "SELECT epoch FROM execution_writer_ownership WHERE consumer='executions' AND owner='archive' AND epoch=$1",
            {n(writerEpoch_)});
    if (PQntuples(owner.get()) != 1) throw std::runtime_error("Execution writer owner/epoch changed; recovery required");
}
void Store::probe() { checkOwnership(false); }

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
    auto nextPosition = position_, nextTrade = trade_;
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
            query("INSERT INTO execution_journal_events(source_identity,position,template_id,payload) "
                  "VALUES($1,$2,$3,decode($4,'hex'))", {source_, n(event.position), n(j.type), payload});
            if (j.type == 1) {
                if (j.trade > nextTrade && j.trade - nextTrade != 1) throw std::runtime_error("Trade gap; checkpoint not advanced");
                if (j.trade <= nextTrade) {
                    auto old = query("SELECT 1 FROM execution_journal_trades WHERE trade_id=$1 AND payload=decode($2,'hex')",
                                     {n(j.trade), payload});
                    if (PQntuples(old.get()) != 1) throw std::runtime_error("Conflicting repeated trade");
                } else {
                    query("INSERT INTO execution_journal_trades(trade_id,payload) VALUES($1,decode($2,'hex'))", {n(j.trade), payload});
                    leg(j, false); leg(j, true); nextTrade = j.trade;
                }
            } else if (j.type == 28) {
                // CONFLICT/CAPACITY are observations, not a new canonical outcome for the id.
                // Preserve their complete raw event without overwriting the original result.
                if (j.result != 2 && j.result != 3) {
                    auto canonical = hex(std::span(event.bytes).subspan(16)); // omit header + delivery seq
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
        query("UPDATE execution_projector_checkpoint SET position=$1,last_trade_id=$2,updated_at=NOW() "
              "WHERE consumer='executions'", {n(nextPosition), n(nextTrade)});
        query("COMMIT");
        position_ = nextPosition; trade_ = nextTrade;
    } catch (...) {
        // Unknown COMMIT outcome terminates this worker. On restart PostgreSQL's
        // committed cursor is authoritative; never re-use speculative in-memory state.
        auto error = std::current_exception();
        try { query("ROLLBACK"); } catch (...) {}
        std::rethrow_exception(error);
    }
}
}
