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
}
