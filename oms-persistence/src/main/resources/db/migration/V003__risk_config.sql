-- SPDX-License-Identifier: Apache-2.0
-- Durable per-market risk config: the FULL effective config map is stored as
-- one JSONB document and replayed through RiskConfigManager.updateConfig at
-- boot. manual_trip records operator circuit-breaker trips so they survive a
-- restart; automatic (price-move) trips are never persisted.
--
-- Apply manually, like V001/V002:
--   psql -U postgres -d oms -v ON_ERROR_STOP=1 -f V003__risk_config.sql

CREATE TABLE IF NOT EXISTS risk_config (
    market_id INT PRIMARY KEY,
    config JSONB NOT NULL,
    manual_trip BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dead since V001: created but never written or read by any code path.
DROP TABLE IF EXISTS risk_config_market;
DROP TABLE IF EXISTS risk_config_user_tier;
