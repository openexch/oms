// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.api.dto.CreateOrderRequest;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Length-delimited canonical request fields, with exact fixed-point integers. */
final class OrderRequestFingerprint {
    private OrderRequestFingerprint() {}
    static String hash(CreateOrderRequest r) {
        try {
            var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
            out.writeInt(1); // canonical format version
            out.writeLong(r.getUserId()); out.writeInt(r.getMarketId());
            text(out, upper(r.getSide())); text(out, upper(r.getOrderType()));
            text(out, r.getTimeInForce() == null ? "GTC" : upper(r.getTimeInForce()));
            out.writeLong(r.getPrice()); out.writeLong(r.getQuantity()); out.writeLong(r.getStopPrice());
            out.writeLong(r.getTrailingDelta()); out.writeLong(r.getDisplayQuantity());
            out.writeLong(r.getExpiresAtMs()); text(out, r.getClientOrderId());
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (IOException | NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    private static String upper(String v) { return v == null ? null : v.toUpperCase(Locale.ROOT); }
    private static void text(DataOutputStream out, String value) throws IOException {
        if (value == null) { out.writeInt(-1); return; }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length); out.write(bytes);
    }
}
