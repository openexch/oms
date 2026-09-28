-- SPDX-License-Identifier: Apache-2.0
-- A request's identity survives terminal orders and OMS restarts. This is separate
-- from client_order_id, whose legacy contract permits reuse after terminal state.
CREATE TABLE oms_order_requests (
    user_id BIGINT NOT NULL,
    request_id VARCHAR(128) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    oms_order_id BIGINT NOT NULL,
    accepted BOOLEAN,
    response_status VARCHAR(32),
    reject_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (user_id, request_id),
    CHECK ((accepted IS NULL AND completed_at IS NULL) OR
           (accepted IS NOT NULL AND response_status IS NOT NULL AND completed_at IS NOT NULL))
);
-- No expiry/automatic deletion: reusing a forgotten key could duplicate an order.
-- Pending rows are unresolved commands, never permission to blindly submit again.
