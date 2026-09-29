-- SPDX-License-Identifier: Apache-2.0
-- Execution writer cutover: the OMS applies projector-committed ME journal events to its live
-- order and risk state. Order rows, positions and this checkpoint commit in one transaction.
CREATE TABLE IF NOT EXISTS oms_journal_consumer_checkpoint (
    consumer TEXT PRIMARY KEY CHECK (consumer = 'oms-live'),
    source_identity TEXT NOT NULL,
    position BIGINT NOT NULL CHECK (position >= 0),
    last_trade_id BIGINT NOT NULL CHECK (last_trade_id >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Net position per user and market as of the checkpoint (BUY adds, SELL subtracts).
CREATE TABLE IF NOT EXISTS oms_risk_positions (
    user_id BIGINT NOT NULL,
    market_id INTEGER NOT NULL,
    net_quantity BIGINT NOT NULL,
    PRIMARY KEY (user_id, market_id)
);
