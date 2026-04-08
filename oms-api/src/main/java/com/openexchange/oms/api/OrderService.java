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

    OrderResponse getOrder(long omsOrderId);

    List<OrderResponse> queryOrders(long userId, String status);

    Map<String, Object> getBalances(long userId);

    List<Map<String, Object>> getMarkets();

    boolean isClusterConnected();

    int getActiveOrderCount();

    void deposit(long userId, int assetId, long amount);

    void withdraw(long userId, int assetId, long amount);
}
