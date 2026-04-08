package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Request DTO for creating an order.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateOrderRequest {

    private long userId;
    private int marketId;
    private String side;         // "BUY" or "SELL"
    private String orderType;    // "LIMIT", "MARKET", "STOP_LOSS", "STOP_LIMIT", "TRAILING_STOP", "ICEBERG"
    private String timeInForce;  // "GTC", "GTD", "IOC", "FOK"
    private double price;
    private double quantity;
    private double stopPrice;
    private double trailingDelta;
    private double displayQuantity;
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

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    public double getQuantity() { return quantity; }
    public void setQuantity(double quantity) { this.quantity = quantity; }

    public double getStopPrice() { return stopPrice; }
    public void setStopPrice(double stopPrice) { this.stopPrice = stopPrice; }

    public double getTrailingDelta() { return trailingDelta; }
    public void setTrailingDelta(double trailingDelta) { this.trailingDelta = trailingDelta; }

    public double getDisplayQuantity() { return displayQuantity; }
    public void setDisplayQuantity(double displayQuantity) { this.displayQuantity = displayQuantity; }

    public long getExpiresAtMs() { return expiresAtMs; }
    public void setExpiresAtMs(long expiresAtMs) { this.expiresAtMs = expiresAtMs; }

    public String getClientOrderId() { return clientOrderId; }
    public void setClientOrderId(String clientOrderId) { this.clientOrderId = clientOrderId; }
}
