-- SPDX-License-Identifier: Apache-2.0
-- Apply with OMS execution writes stopped and V007 ownership migration installed. Conflicting legacy duplicates must
-- be investigated; this migration deliberately fails instead of deleting them.
BEGIN;
CREATE UNIQUE INDEX IF NOT EXISTS executions_trade_leg ON executions(trade_id, is_maker);
CREATE TABLE IF NOT EXISTS execution_projector_checkpoint (
    consumer TEXT PRIMARY KEY CHECK (consumer = 'executions'),
    source_identity TEXT NOT NULL,
    recording_descriptor TEXT NOT NULL,
    position BIGINT NOT NULL CHECK (position >= 0),
    last_trade_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS execution_journal_events (
    source_identity TEXT NOT NULL,
    position BIGINT NOT NULL,
    template_id SMALLINT NOT NULL,
    payload BYTEA NOT NULL,
    PRIMARY KEY(source_identity, position)
);
CREATE TABLE IF NOT EXISTS execution_journal_trades (
    trade_id BIGINT PRIMARY KEY,
    payload BYTEA NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_journal_terminals (
    source_identity TEXT NOT NULL,
    position BIGINT NOT NULL,
    oms_order_id BIGINT NOT NULL,
    cluster_order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    market_id INTEGER NOT NULL,
    status SMALLINT NOT NULL,
    event_time_ms BIGINT NOT NULL,
    PRIMARY KEY(source_identity, position),
    FOREIGN KEY(source_identity, position) REFERENCES execution_journal_events
);
CREATE INDEX IF NOT EXISTS execution_terminal_order ON execution_journal_terminals(oms_order_id, position);
COMMIT;
