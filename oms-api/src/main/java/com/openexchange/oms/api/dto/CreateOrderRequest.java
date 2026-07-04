// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Request DTO for creating an order.
 *
 * Money fields are internal 8-dp fixed-point longs; on the wire they are
 * exact decimal strings (oms#39). Legacy JSON numbers are still accepted
 * (deprecated) via {@link FixedPointJson.Deserializer}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateOrderRequest {

    private long userId;
    private int marketId;
    private String side;         // "BUY" or "SELL"
    private String orderType;    // "LIMIT", "MARKET", "STOP_LOSS", "STOP_LIMIT", "TRAILING_STOP", "ICEBERG"
    private String timeInForce;  // "GTC", "GTD", "IOC", "FOK"
    @JsonDeserialize(using = FixedPointJson.Deserializer.class)
    private long price;
    @JsonDeserialize(using = FixedPointJson.Deserializer.class)
    private long quantity;
    @JsonDeserialize(using = FixedPointJson.Deserializer.class)
    private long stopPrice;
    @JsonDeserialize(using = FixedPointJson.Deserializer.class)
    private long trailingDelta;
    @JsonDeserialize(using = FixedPointJson.Deserializer.class)
    private long displayQuantity;
    private long expiresAtMs;
    private String clientOrderId; // User-supplied idempotency key

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public int getMarketId() { return marketId; }
    public void setMarketId(int marketId) { this.marketId = marketId; }

    public String getSide() { return side; }
    public void setSide(String side) { this.side = side; }

    public String getOrderType() { return orderType; }
    public void setOrderType(String orderType) { this.orderType = orderType; }

    public String getTimeInForce() { return timeInForce; }
    public void setTimeInForce(String timeInForce) { this.timeInForce = timeInForce; }

    public long getPrice() { return price; }
    public void setPrice(long price) { this.price = price; }

    public long getQuantity() { return quantity; }
    public void setQuantity(long quantity) { this.quantity = quantity; }

    public long getStopPrice() { return stopPrice; }
    public void setStopPrice(long stopPrice) { this.stopPrice = stopPrice; }

    public long getTrailingDelta() { return trailingDelta; }
    public void setTrailingDelta(long trailingDelta) { this.trailingDelta = trailingDelta; }

    public long getDisplayQuantity() { return displayQuantity; }
    public void setDisplayQuantity(long displayQuantity) { this.displayQuantity = displayQuantity; }

    public long getExpiresAtMs() { return expiresAtMs; }
    public void setExpiresAtMs(long expiresAtMs) { this.expiresAtMs = expiresAtMs; }

    public String getClientOrderId() { return clientOrderId; }
    public void setClientOrderId(String clientOrderId) { this.clientOrderId = clientOrderId; }
}
