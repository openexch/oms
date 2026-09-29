-- SPDX-License-Identifier: Apache-2.0
-- ABORTED is terminal: the OMS never offered the command, so its hold may be released.
ALTER TABLE oms_me_commands DROP CONSTRAINT IF EXISTS oms_me_commands_state_check;
ALTER TABLE oms_me_commands ADD CONSTRAINT oms_me_commands_state_check
    CHECK(state IN ('PREPARED','READY','RESOLVED','ABORTED'));
CREATE INDEX IF NOT EXISTS oms_me_commands_open ON oms_me_commands(command_id_high,command_id_low)
    WHERE state IN ('PREPARED','READY');
