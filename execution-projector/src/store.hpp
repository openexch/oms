// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "journal.hpp"
#include <libpq-fe.h>
#include <memory>

namespace oe {
struct ResultDeleter { void operator()(PGresult* p) const { PQclear(p); } };
using Result = std::unique_ptr<PGresult, ResultDeleter>;
class Store {
public:
    Store(const std::string& conninfo, std::string source, std::string descriptor);
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;
    std::int64_t position() const { return position_; }
    // RC2 S3: the producer cursor (contract 10.2), committed with position.
    std::int64_t lastJournalSeq() const { return seq_; }
    std::int64_t meEpoch() const { return epoch_; }
    // Duplicate journalSeqs skipped after a rebase (projector_duplicate_seq_total), this process.
    std::int64_t duplicateSeqs() const { return duplicates_; }
    // RC2 S3 --rebase-source (contract 10.5): replace source and descriptor, reset position to 0,
    // keep last_journal_seq, me_epoch and last_trade_id. Refused while a worker holds the
    // checkpoint (the writer advisory lock), without archive ownership, without a seq cursor, or
    // onto a source identity that already has journal rows.
    static void rebase(const std::string& conninfo, const std::string& source, const std::string& descriptor);
    void apply(std::span<const Event> batch);
    void probe();
    void observe(std::int64_t recordedTarget);
private:
    PGconn* db_{};
    std::string source_;
    std::int64_t position_{}, trade_{}, writerEpoch_{};
    std::int64_t seq_{}, epoch_{}, ceiling_{}, duplicates_{};
    void checkOwnership(bool lock);
    Result query(const char* sql, const std::vector<std::string>& values = {});
    void leg(const Journal& j, bool maker);
};
}
