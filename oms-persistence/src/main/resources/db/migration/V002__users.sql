-- SPDX-License-Identifier: Apache-2.0
-- Demo user accounts (OMS_AUTH_MODE=demo): simple username/password registration
-- with one active opaque bearer token per user.
--
-- userId allocation: 100000-899999. Positive (createOrder rejects <= 0), never
-- 0 (Principal.resolveUserId treats 0 as "unspecified") or 1 (dev default),
-- and hard-capped below the market-sim bot/canary range (900001-900999).
--
-- Apply manually, like V001:
--   psql -U postgres -d oms -v ON_ERROR_STOP=1 -f V002__users.sql

CREATE SEQUENCE IF NOT EXISTS demo_user_seq START 100000 MAXVALUE 899999 NO CYCLE;

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY DEFAULT nextval('demo_user_seq'),
    -- lowercase [a-z0-9_]{3,20}, validated at the API layer
    username VARCHAR(32) UNIQUE NOT NULL,
    -- self-describing: pbkdf2$<iterations>$<saltB64>$<hashB64>
    password_hash VARCHAR(256) NOT NULL,
    -- current opaque bearer token (URL-safe base64); one active session per
    -- user, rotated on every login
    token VARCHAR(64) UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS users_token_idx ON users (token);
