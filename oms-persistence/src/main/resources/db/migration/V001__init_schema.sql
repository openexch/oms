CREATE TABLE account_balances (
    user_id BIGINT NOT NULL,
    asset_id INT NOT NULL,
    available BIGINT NOT NULL DEFAULT 0,
    locked BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, asset_id)
);

CREATE TABLE ledger_entries (
    entry_id BIGSERIAL PRIMARY KEY,
    journal_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    asset_id INT NOT NULL,
    amount BIGINT NOT NULL,
    entry_type VARCHAR(32) NOT NULL,
    is_debit BOOLEAN NOT NULL,
    reference_id BIGINT,
    secondary_ref BIGINT,
    entry_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (entry_time);

-- Create initial partitions for current and next month
CREATE TABLE ledger_entries_default PARTITION OF ledger_entries DEFAULT;

CREATE INDEX idx_ledger_journal ON ledger_entries(journal_id);
CREATE INDEX idx_ledger_user ON ledger_entries(user_id, entry_time DESC);

CREATE TABLE orders (
    oms_order_id BIGINT PRIMARY KEY,
    cluster_order_id BIGINT,
    client_order_id VARCHAR(64),
    user_id BIGINT NOT NULL,
    market_id INT NOT NULL,
    side VARCHAR(4) NOT NULL,
    order_type VARCHAR(16) NOT NULL,
    time_in_force VARCHAR(4) NOT NULL DEFAULT 'GTC',
    price BIGINT NOT NULL DEFAULT 0,
    quantity BIGINT NOT NULL,
    filled_qty BIGINT NOT NULL DEFAULT 0,
    remaining_qty BIGINT NOT NULL,
    stop_price BIGINT DEFAULT 0,
    trailing_delta BIGINT DEFAULT 0,
    display_quantity BIGINT DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    reject_reason VARCHAR(64),
    hold_amount BIGINT DEFAULT 0,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_user_status ON orders(user_id, status);
CREATE INDEX idx_orders_market_status ON orders(market_id, status);
CREATE INDEX idx_orders_client ON orders(client_order_id) WHERE client_order_id IS NOT NULL;

CREATE TABLE executions (
    execution_id BIGSERIAL PRIMARY KEY,
    trade_id BIGINT NOT NULL,
    oms_order_id BIGINT NOT NULL REFERENCES orders(oms_order_id),
    user_id BIGINT NOT NULL,
    market_id INT NOT NULL,
    side VARCHAR(4) NOT NULL,
    price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    is_maker BOOLEAN NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_executions_order ON executions(oms_order_id);
CREATE INDEX idx_executions_user ON executions(user_id, executed_at DESC);

CREATE TABLE balance_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    asset_id INT NOT NULL,
    available BIGINT NOT NULL,
    locked BIGINT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE risk_config_market (
    market_id INT PRIMARY KEY,
    min_quantity BIGINT NOT NULL,
    max_quantity BIGINT NOT NULL,
    min_notional BIGINT NOT NULL,
    max_notional BIGINT NOT NULL,
    price_collar_pct INT NOT NULL DEFAULT 10,
    circuit_breaker_pct INT NOT NULL DEFAULT 20,
    circuit_breaker_window_ms BIGINT NOT NULL DEFAULT 60000
);

CREATE TABLE risk_config_user_tier (
    tier VARCHAR(16) PRIMARY KEY,
    max_orders_per_sec INT NOT NULL DEFAULT 10,
    max_orders_per_min INT NOT NULL DEFAULT 300,
    max_open_orders INT NOT NULL DEFAULT 200,
    max_position_per_market BIGINT NOT NULL DEFAULT 0
);
