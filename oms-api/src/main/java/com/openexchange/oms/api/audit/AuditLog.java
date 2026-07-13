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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Append-only audit log of order + account + admin mutations (oms#37).
 *
 * One JSON object per line: ts, action, subject, userId, resource, success,
 * detail. Opened in append mode and flushed per record so entries survive a
 * crash; writes are synchronized (mutations are edge-rate, not hot-path).
 * OMS_AUDIT_LOG names the file ("off" disables); parent directories are
 * created on open.
 *
 * Size-capped rotation: when the file reaches {@link #DEFAULT_MAX_BYTES} it is
 * rolled to {@code <file>.1} (existing rolled files shift to {@code .2}..{@code .5};
 * the oldest is dropped) and a fresh file is opened, so the audit trail is
 * bounded at ~600MB total instead of growing without limit.
 */
public final class AuditLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Roll the active file once it reaches this many bytes. */
    static final long DEFAULT_MAX_BYTES = 100L * 1024 * 1024;
    /** Rolled files kept: <file>.1 (newest) .. <file>.5 (oldest). */
    static final int MAX_ROLLED_FILES = 5;

    private static final int NEWLINE_BYTES = System.lineSeparator()
            .getBytes(StandardCharsets.UTF_8).length;

    private final Path file;   // null = disabled
    private final long maxBytes;
    private BufferedWriter writer; // null = disabled (or reopen after rotation failed)
    private long bytesWritten;
    private boolean writeFailed;

    private AuditLog(Path file, BufferedWriter writer, long maxBytes, long existingSize) {
        this.file = file;
        this.writer = writer;
        this.maxBytes = maxBytes;
        this.bytesWritten = existingSize;
    }

    public static AuditLog open(String path) {
        return open(path, DEFAULT_MAX_BYTES);
    }

    /** Visible for tests: open with a custom rotation threshold. */
    static AuditLog open(String path, long maxBytes) {
        if (path == null || path.isBlank() || path.equalsIgnoreCase("off") || path.equalsIgnoreCase("none")) {
            log.warn("AUDIT: disabled (OMS_AUDIT_LOG={})", path);
            return new AuditLog(null, null, maxBytes, 0);
        }
        try {
            Path file = Path.of(path);
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            BufferedWriter writer = newWriter(file);
            long existingSize = Files.size(file);
            log.info("AUDIT: appending to {} (rotating at {} bytes, keeping {} rolled files)",
                    file.toAbsolutePath(), maxBytes, MAX_ROLLED_FILES);
            return new AuditLog(file, writer, maxBytes, existingSize);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot open audit log " + path + ": " + e.getMessage(), e);
        }
    }

    public static AuditLog disabled() {
        return new AuditLog(null, null, DEFAULT_MAX_BYTES, 0);
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
                if (writer == null) return;
                writer.write(line);
                writer.newLine();
                writer.flush();
                bytesWritten += line.getBytes(StandardCharsets.UTF_8).length + NEWLINE_BYTES;
                if (bytesWritten >= maxBytes) {
                    rotate();
                }
            }
        } catch (IOException e) {
            if (!writeFailed) {
                writeFailed = true;
                log.error("AUDIT: write failed (subsequent failures suppressed): {}", e.getMessage());
            }
        }
    }

    /**
     * Rolls the active file to {@code <file>.1}, shifting existing rolled files up and
     * dropping the oldest, then reopens a fresh active file. On a failed shift the
     * counter still restarts so a persistent FS problem logs one error per
     * {@code maxBytes} written, not one per record.
     */
    private void rotate() {
        try {
            writer.close();
            Files.deleteIfExists(rolled(MAX_ROLLED_FILES));
            for (int i = MAX_ROLLED_FILES - 1; i >= 1; i--) {
                Path from = rolled(i);
                if (Files.exists(from)) {
                    Files.move(from, rolled(i + 1), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            Files.move(file, rolled(1), StandardCopyOption.REPLACE_EXISTING);
            log.info("AUDIT: rotated {} (keeping {} rolled files)", file, MAX_ROLLED_FILES);
        } catch (IOException e) {
            log.error("AUDIT: rotation of {} failed: {}", file, e.getMessage());
        }
        bytesWritten = 0;
        try {
            writer = newWriter(file);
        } catch (IOException e) {
            writer = null; // auditing stops rather than throwing on the request path
            log.error("AUDIT: cannot reopen {} after rotation: {}", file, e.getMessage());
        }
    }

    private Path rolled(int i) {
        return file.resolveSibling(file.getFileName() + "." + i);
    }

    private static BufferedWriter newWriter(Path file) throws IOException {
        return Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) writer.close();
    }
}
