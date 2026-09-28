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
    void apply(std::span<const Event> batch);
    void probe() { query("SELECT 1"); }
private:
    PGconn* db_{};
    std::string source_;
    std::int64_t position_{}, trade_{};
    Result query(const char* sql, const std::vector<std::string>& values = {});
    void leg(const Journal& j, bool maker);
};
}
