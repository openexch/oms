-- SPDX-License-Identifier: Apache-2.0
-- Drop the dead double-entry ledger tables. The Assets Engine is the book of
-- record for money movements; these tables were created by V001 for a derived
-- ledger journal that was never wired up (no writer was ever instantiated),
-- so both have been empty since genesis. The writing/reading code
-- (PostgresLedgerRepository, PersistenceBatchWriter, LedgerEntry) is removed
-- in the same change.
--
-- account_balances, users, orders, and executions are untouched.
--
-- Apply manually, like V001/V002:
--   psql -U postgres -d oms -v ON_ERROR_STOP=1 -f V004__drop_dead_ledger_tables.sql

-- ledger_entries is partitioned (V001 created the DEFAULT partition);
-- drop the partition explicitly, then the parent.
DROP TABLE IF EXISTS ledger_entries_default;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS balance_snapshots;
