-- SPDX-License-Identifier: Apache-2.0
-- PREPARED is durable intent only; it MUST NOT be sent until the hold/workflow precondition is proven.
CREATE TABLE IF NOT EXISTS oms_me_commands (
    command_id_high BIGINT NOT NULL,
    command_id_low BIGINT NOT NULL,
    oms_order_id BIGINT NOT NULL,
    workflow_revision BIGINT NOT NULL CHECK(workflow_revision>0),
    command_kind SMALLINT NOT NULL CHECK(command_kind BETWEEN 0 AND 2),
    payload BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'PREPARED' CHECK(state IN ('PREPARED','READY','RESOLVED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY(command_id_high,command_id_low),
    UNIQUE(oms_order_id,workflow_revision,command_kind)
);
CREATE INDEX IF NOT EXISTS oms_me_commands_pending ON oms_me_commands(created_at) WHERE state='READY';
