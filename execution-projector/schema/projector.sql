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
-- Freshness for consumers on other hosts: the recorded journal end this worker last observed,
-- stamped with database time so readers compare against NOW() without trusting any host clock.
ALTER TABLE execution_projector_checkpoint ADD COLUMN IF NOT EXISTS observed_target BIGINT NOT NULL DEFAULT 0;
ALTER TABLE execution_projector_checkpoint ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ;
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
-- Command results never imply a settlement or parent-order terminal. Repeated replies carry
-- the original applied position/time. Only the journal delivery position changes on a retry.
CREATE TABLE IF NOT EXISTS me_command_outcomes (
    command_id_high BIGINT NOT NULL,
    command_id_low BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    oms_order_id BIGINT NOT NULL,
    market_id INTEGER NOT NULL,
    command_kind SMALLINT NOT NULL,
    old_order_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    old_cancelled BOOLEAN NOT NULL,
    status INTEGER NOT NULL,
    reason INTEGER NOT NULL,
    result INTEGER NOT NULL CHECK(result IN (0,1,4,5,6)),
    applied_position BIGINT NOT NULL,
    event_time_ms BIGINT NOT NULL,
    source_identity TEXT NOT NULL,
    first_position BIGINT NOT NULL,
    canonical_payload BYTEA NOT NULL,
    PRIMARY KEY(command_id_high,command_id_low),
    FOREIGN KEY(source_identity,first_position) REFERENCES execution_journal_events
);
CREATE INDEX IF NOT EXISTS me_command_outcomes_parent ON me_command_outcomes(oms_order_id);
COMMIT;
