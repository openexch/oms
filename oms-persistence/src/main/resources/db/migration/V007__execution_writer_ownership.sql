-- SPDX-License-Identifier: Apache-2.0
-- Install before starting an Archive projector. Default preserves the legacy owner;
-- an explicit paused boundary is required to change owners.
CREATE TABLE execution_writer_ownership (
    consumer TEXT PRIMARY KEY CHECK (consumer = 'executions'),
    owner TEXT NOT NULL CHECK (owner IN ('legacy', 'paused', 'archive')),
    epoch BIGINT NOT NULL CHECK (epoch > 0)
);
INSERT INTO execution_writer_ownership VALUES ('executions', 'legacy', 1);

CREATE TABLE execution_writer_handoffs (
    epoch BIGINT PRIMARY KEY,
    previous_owner TEXT NOT NULL,
    next_owner TEXT NOT NULL,
    reason TEXT NOT NULL,
    changed_by TEXT NOT NULL DEFAULT CURRENT_USER,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE FUNCTION transition_execution_writer(expected_owner TEXT, expected_epoch BIGINT,
                                            next_owner TEXT, reason TEXT) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE current_owner TEXT; current_epoch BIGINT;
BEGIN
    IF reason IS NULL OR length(trim(reason)) = 0 THEN
        RAISE EXCEPTION 'Writer handoff requires an audit reason' USING ERRCODE = '55000';
    END IF;
    SELECT o.owner, o.epoch INTO STRICT current_owner, current_epoch
      FROM execution_writer_ownership o WHERE consumer = 'executions' FOR UPDATE;
    IF current_owner <> expected_owner OR current_epoch <> expected_epoch THEN
        RAISE EXCEPTION 'Stale execution writer handoff' USING ERRCODE = '55000';
    END IF;
    IF NOT ((current_owner IN ('legacy','archive') AND next_owner = 'paused')
         OR (current_owner = 'paused' AND next_owner IN ('legacy','archive'))) THEN
        RAISE EXCEPTION 'Execution writer handoff requires paused boundary' USING ERRCODE = '55000';
    END IF;
    UPDATE execution_writer_ownership SET owner = next_owner, epoch = current_epoch + 1
      WHERE consumer = 'executions';
    INSERT INTO execution_writer_handoffs(epoch, previous_owner, next_owner, reason)
      VALUES(current_epoch + 1, current_owner, next_owner, reason);
    RETURN current_epoch + 1;
END $$;

CREATE FUNCTION guard_execution_writer() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE current_owner TEXT; current_epoch BIGINT;
        writer TEXT := COALESCE(NULLIF(current_setting('oe.execution_writer', true), ''), 'legacy');
        writer_epoch TEXT := current_setting('oe.execution_writer_epoch', true);
BEGIN
    -- Hold through COMMIT. A handoff waits for in-flight writes, and a write
    -- beginning after the handoff observes the new owner/epoch.
    SELECT o.owner, o.epoch INTO STRICT current_owner, current_epoch
      FROM execution_writer_ownership o WHERE consumer = 'executions' FOR SHARE;
    IF current_owner = 'paused' OR writer <> current_owner
       OR (writer = 'archive' AND writer_epoch IS DISTINCT FROM current_epoch::TEXT) THEN
        RAISE EXCEPTION 'Execution writer fenced' USING ERRCODE = '55000';
    END IF;
    RETURN NULL; -- Statement trigger, not a row filter.
END $$;
CREATE TRIGGER execution_writer_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON executions
    FOR EACH STATEMENT EXECUTE FUNCTION guard_execution_writer();
