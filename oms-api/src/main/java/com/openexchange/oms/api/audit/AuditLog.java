// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openexchange.oms.api.auth.Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Append-only audit log of order + account + admin mutations (oms#37).
 *
 * One JSON object per line: ts, action, subject, userId, resource, success,
 * detail. Opened in append mode and flushed per record so entries survive a
 * crash; writes are synchronized (mutations are edge-rate, not hot-path).
 * OMS_AUDIT_LOG names the file ("off" disables).
 */
public final class AuditLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferedWriter writer; // null = disabled
    private boolean writeFailed;

    private AuditLog(BufferedWriter writer) {
        this.writer = writer;
    }

    public static AuditLog open(String path) {
        if (path == null || path.isBlank() || path.equalsIgnoreCase("off") || path.equalsIgnoreCase("none")) {
            log.warn("AUDIT: disabled (OMS_AUDIT_LOG={})", path);
            return new AuditLog(null);
        }
        try {
            Path file = Path.of(path);
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.info("AUDIT: appending to {}", file.toAbsolutePath());
            return new AuditLog(writer);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot open audit log " + path + ": " + e.getMessage(), e);
        }
    }

    public static AuditLog disabled() {
        return new AuditLog(null);
    }

    public void record(Principal principal, String action, String resource, boolean success, String detail) {
        if (writer == null) return;
        try {
            ObjectNode entry = MAPPER.createObjectNode();
            entry.put("ts", Instant.now().toString());
            entry.put("action", action);
            entry.put("subject", principal != null ? principal.subject() : null);
            if (principal != null) entry.put("userId", principal.userId());
            entry.put("resource", resource);
            entry.put("success", success);
            if (detail != null) entry.put("detail", detail);
            String line = MAPPER.writeValueAsString(entry);
            synchronized (this) {
                writer.write(line);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            if (!writeFailed) {
                writeFailed = true;
                log.error("AUDIT: write failed (subsequent failures suppressed): {}", e.getMessage());
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) writer.close();
    }
}
