// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openexchange.oms.api.auth.Principal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AuditLogTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void appendsOneJsonLinePerRecord(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("audit/oms-audit.log");
        Principal principal = new Principal("api-key-user-7", 7, Set.of());

        try (AuditLog audit = AuditLog.open(file.toString())) {
            audit.record(principal, "order.create", "user:7", true, "omsOrderId=42");
            audit.record(principal, "account.withdraw", "user:7", false, "Insufficient balance");
        }

        List<String> lines = Files.readAllLines(file);
        assertEquals(2, lines.size());

        JsonNode first = MAPPER.readTree(lines.get(0));
        assertEquals("order.create", first.get("action").asText());
        assertEquals("api-key-user-7", first.get("subject").asText());
        assertEquals(7, first.get("userId").asLong());
        assertTrue(first.get("success").asBoolean());
        assertTrue(first.hasNonNull("ts"));

        JsonNode second = MAPPER.readTree(lines.get(1));
        assertFalse(second.get("success").asBoolean());
        assertEquals("Insufficient balance", second.get("detail").asText());
    }

    @Test
    void appendsAcrossReopens(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("audit.log");
        try (AuditLog audit = AuditLog.open(file.toString())) {
            audit.record(null, "admin.risk.update", "market:1", true, null);
        }
        try (AuditLog audit = AuditLog.open(file.toString())) {
            audit.record(null, "admin.risk.update", "market:2", true, null);
        }
        assertEquals(2, Files.readAllLines(file).size(), "append-only across restarts");
    }

    @Test
    void offDisables(@TempDir Path dir) throws Exception {
        try (AuditLog audit = AuditLog.open("off")) {
            audit.record(null, "order.create", "user:1", true, null);
        }
        assertEquals(0, Files.list(dir).count());
    }

    @Test
    void rotatesAtSizeCapKeepingFiveRolledFiles(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("audit.log");
        // 1-byte cap: every record trips a roll, so 8 records exercise the shift
        // chain past the retention cap.
        try (AuditLog audit = AuditLog.open(file.toString(), 1)) {
            for (int i = 0; i < 8; i++) {
                audit.record(null, "action." + i, "user:1", true, null);
            }
        }

        // Active file was reopened fresh after the last roll.
        assertTrue(Files.exists(file));
        assertEquals(0, Files.size(file));

        // Exactly 5 rolled files survive; the 6th-oldest and beyond are dropped.
        for (int i = 1; i <= 5; i++) {
            assertTrue(Files.exists(dir.resolve("audit.log." + i)), "audit.log." + i + " must exist");
        }
        assertFalse(Files.exists(dir.resolve("audit.log.6")));

        // .1 is the newest rolled file, .5 the oldest survivor (records 0-2 dropped).
        JsonNode newest = MAPPER.readTree(Files.readAllLines(dir.resolve("audit.log.1")).get(0));
        assertEquals("action.7", newest.get("action").asText());
        JsonNode oldest = MAPPER.readTree(Files.readAllLines(dir.resolve("audit.log.5")).get(0));
        assertEquals("action.3", oldest.get("action").asText());
    }

    @Test
    void preGrownFileRollsOnFirstRecordOverCap(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("audit.log");
        Files.writeString(file, "already-big\n".repeat(10)); // 120 bytes > 64-byte cap

        try (AuditLog audit = AuditLog.open(file.toString(), 64)) {
            audit.record(null, "order.create", "user:1", true, null);
        }

        assertTrue(Files.exists(dir.resolve("audit.log.1")), "over-cap file must roll on the first record");
        assertEquals(0, Files.size(file), "fresh active file after the roll");
        List<String> rolled = Files.readAllLines(dir.resolve("audit.log.1"));
        assertEquals("order.create", MAPPER.readTree(rolled.get(rolled.size() - 1)).get("action").asText(),
                "the record that tripped the roll lands in the rolled file");
    }
}
