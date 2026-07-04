// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Response DTO for the positions read (oms#40): net base-asset quantity per
 * market, aggregated from the executions ledger. Signed — negative means
 * net sold. Money as exact 8-dp decimal strings (oms#39).
 */
public class PositionResponse {

    private long userId;
    private int marketId;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long netQuantity;

    public PositionResponse() {}

    public PositionResponse(long userId, int marketId, long netQuantity) {
        this.userId = userId;
        this.marketId = marketId;
        this.netQuantity = netQuantity;
    }

    public long getUserId() { return userId; }
    public int getMarketId() { return marketId; }
    public long getNetQuantity() { return netQuantity; }
}
