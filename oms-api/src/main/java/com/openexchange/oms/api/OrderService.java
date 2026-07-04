// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api;

import com.openexchange.oms.api.dto.*;

import java.util.List;
import java.util.Map;

/**
 * Service interface for order operations.
 * Implemented by the main application wiring layer.
 */
public interface OrderService {

    CreateOrderResponse createOrder(CreateOrderRequest request);

    CancelOrderResponse cancelOrder(long omsOrderId);

    /** Prices/quantities are 8-dp fixed-point longs; 0 = keep the current value. */
    Map<String, Object> updateOrder(long omsOrderId, long newPrice, long newQuantity);

    OrderResponse getOrder(long omsOrderId);

    List<OrderResponse> queryOrders(long userId, String status);

    /**
     * Order history from persistence, newest first (oms#40). {@code status}
     * null = all statuses. Throws IllegalStateException when persistence is
     * not configured, IllegalArgumentException on an unknown status.
     */
    List<OrderResponse> getOrderHistory(long userId, String status, int limit, int offset);

    /** Execution (fill) history from persistence, newest first (oms#40). */
    List<ExecutionResponse> getExecutions(long userId, int limit, int offset);

    /** Net position per market aggregated from the executions ledger (oms#40). */
    List<PositionResponse> getPositions(long userId);

    Map<String, Object> getBalances(long userId);

    List<Map<String, Object>> getMarkets();

    boolean isClusterConnected();

    int getActiveOrderCount();

    void deposit(long userId, int assetId, long amount);

    void withdraw(long userId, int assetId, long amount);
}
