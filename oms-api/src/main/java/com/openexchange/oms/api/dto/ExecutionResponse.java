// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.openexchange.oms.common.domain.ExecutionReport;

/**
 * Response DTO for execution (trade fill) history reads (oms#40).
 *
 * Same wire conventions as OrderResponse (oms#39): money as exact 8-dp
 * decimal strings, 64-bit ids as JSON strings.
 */
public class ExecutionResponse {

    @JsonSerialize(using = ToStringSerializer.class)
    private long tradeId;
    @JsonSerialize(using = ToStringSerializer.class)
    private long omsOrderId;
    private long userId;
    private int marketId;
    private String side;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long price;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long quantity;
    private boolean maker;
    private long executedAtMs;

    public static ExecutionResponse fromExecution(ExecutionReport exec) {
        ExecutionResponse r = new ExecutionResponse();
        r.tradeId = exec.getTradeId();
        r.omsOrderId = exec.getOmsOrderId();
        r.userId = exec.getUserId();
        r.marketId = exec.getMarketId();
        r.side = exec.getSide() != null ? exec.getSide().name() : null;
        r.price = exec.getPrice();
        r.quantity = exec.getQuantity();
        r.maker = exec.isMaker();
        r.executedAtMs = exec.getExecutedAtMs();
        return r;
    }

    public long getTradeId() { return tradeId; }
    public long getOmsOrderId() { return omsOrderId; }
    public long getUserId() { return userId; }
    public int getMarketId() { return marketId; }
    public String getSide() { return side; }
    public long getPrice() { return price; }
    public long getQuantity() { return quantity; }
    public boolean isMaker() { return maker; }
    public long getExecutedAtMs() { return executedAtMs; }
}
