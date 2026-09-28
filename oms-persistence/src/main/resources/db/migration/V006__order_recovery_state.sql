-- SPDX-License-Identifier: Apache-2.0
-- Existing rows retain revision zero: these defaults cannot reconstruct lost workflow history.
ALTER TABLE orders
    ADD COLUMN trailing_arm_price BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN hidden_quantity BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN slice_remaining_qty BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN hold_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN parent_oms_order_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN replace_pending_old_cluster_order_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN pending_price BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN pending_quantity BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN pending_hold_delta BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN pending_hold_target BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN replace_requested_at_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN pending_hold_requested BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN state_revision BIGINT NOT NULL DEFAULT 0 CHECK (state_revision >= 0);
