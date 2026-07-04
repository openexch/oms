// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Static API-key registry (OMS_AUTH_MODE=api-key, the secure default).
 *
 * Entry format: {@code key:userId[:role1|role2]}. Entries separated by ';' or
 * newlines; '#' starts a comment line. Sources: OMS_API_KEYS (inline) and/or
 * OMS_API_KEYS_FILE. With no keys configured every request is rejected —
 * secure by default.
 *
 * Keys are matched constant-time; the registry is immutable after startup.
 */
public final class ApiKeyAuthenticationProvider implements AuthenticationProvider {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyAuthenticationProvider.class);

    private record Entry(byte[] key, Principal principal) {}

    private final List<Entry> entries;

    private ApiKeyAuthenticationProvider(List<Entry> entries) {
        this.entries = entries;
    }

    /** Build from the inline spec and/or key file; either may be blank. */
    public static ApiKeyAuthenticationProvider parse(String inlineSpec, String filePath) {
        List<Entry> entries = new ArrayList<>();
        if (inlineSpec != null && !inlineSpec.isBlank()) {
            parseInto(inlineSpec, entries);
        }
        if (filePath != null && !filePath.isBlank()) {
            try {
                parseInto(Files.readString(Path.of(filePath), StandardCharsets.UTF_8), entries);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot read OMS_API_KEYS_FILE " + filePath + ": " + e.getMessage(), e);
            }
        }
        log.info("API-key auth: {} key(s) registered", entries.size());
        return new ApiKeyAuthenticationProvider(entries);
    }

    private static void parseInto(String spec, List<Entry> entries) {
        for (String line : spec.split("[;\n]")) {
            String entry = line.trim();
            if (entry.isEmpty() || entry.startsWith("#")) continue;
            String[] parts = entry.split(":");
            if (parts.length < 2 || parts.length > 3 || parts[0].isEmpty()) {
                throw new IllegalArgumentException("Bad API key entry (want key:userId[:role1|role2]): "
                        + entry.substring(0, Math.min(8, entry.length())) + "...");
            }
            long userId = Long.parseLong(parts[1].trim());
            Set<String> roles = new HashSet<>();
            if (parts.length == 3) {
                for (String role : parts[2].split("\\|")) {
                    if (!role.isBlank()) roles.add(role.trim().toUpperCase());
                }
            }
            entries.add(new Entry(parts[0].getBytes(StandardCharsets.UTF_8),
                    new Principal("api-key-user-" + userId, userId, Set.copyOf(roles))));
        }
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public Principal authenticate(Headers headers) throws AuthenticationException {
        String token = AuthenticationProvider.extractToken(headers);
        if (token == null || token.isEmpty()) {
            throw new AuthenticationException("Missing credentials (Authorization: Bearer <key> or X-API-Key)");
        }
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
        Principal matched = null;
        for (Entry entry : entries) {
            // Constant-time compare over every entry — no early exit on match.
            if (MessageDigest.isEqual(entry.key, tokenBytes)) {
                matched = entry.principal;
            }
        }
        if (matched == null) {
            throw new AuthenticationException("Unknown API key");
        }
        return matched;
    }
}
