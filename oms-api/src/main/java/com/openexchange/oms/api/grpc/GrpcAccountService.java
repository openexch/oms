// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.grpc;

import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.Authorizer;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import com.openexchange.oms.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * gRPC implementation of AccountService.
 */
public class GrpcAccountService extends AccountServiceGrpc.AccountServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(GrpcAccountService.class);

    private final OrderService orderService;
    private final Authorizer authorizer;
    private final Map<Long, Set<StreamObserver<BalanceUpdate>>> balanceStreamsByUser = new ConcurrentHashMap<>();

    public GrpcAccountService(OrderService orderService) {
        this(orderService, new RoleBasedAuthorizer());
    }

    public GrpcAccountService(OrderService orderService, Authorizer authorizer) {
        this.orderService = orderService;
        this.authorizer = authorizer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void getBalances(GetBalancesRequest request,
                             StreamObserver<BalancesResponse> responseObserver) {
        try {
            Long allowed = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
            if (allowed == null) return;
            long userId = allowed;
            Map<String, Object> balances = orderService.getBalances(userId);

            BalancesResponse.Builder builder = BalancesResponse.newBuilder()
                    .setUserId(userId);

            Object assets = balances.get("assets");
            if (assets instanceof List<?> assetList) {
                for (Object item : assetList) {
                    if (item instanceof Map<?, ?> assetMap) {
                        builder.addAssets(AssetBalance.newBuilder()
                                .setAsset(String.valueOf(assetMap.get("asset")))
                                .setAssetId(((Number) assetMap.get("assetId")).intValue())
                                .setAvailable(String.valueOf(assetMap.get("available")))
                                .setLocked(String.valueOf(assetMap.get("locked")))
                                .setTotal(String.valueOf(assetMap.get("total")))
                                .build());
                    }
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("gRPC getBalances failed", e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void streamBalances(StreamRequest request, StreamObserver<BalanceUpdate> responseObserver) {
        Long allowed = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
        if (allowed == null) return;
        long userId = allowed;
        Set<StreamObserver<BalanceUpdate>> observers =
                balanceStreamsByUser.computeIfAbsent(userId, k -> new CopyOnWriteArraySet<>());
        observers.add(responseObserver);

        if (responseObserver instanceof ServerCallStreamObserver<BalanceUpdate> sco) {
            sco.setOnCancelHandler(() -> {
                observers.remove(responseObserver);
                if (observers.isEmpty()) balanceStreamsByUser.remove(userId);
            });
        }
    }

    /**
     * Push a balance update to all streaming subscribers for a user.
     */
    public void pushBalanceUpdate(long userId, BalanceUpdate update) {
        Set<StreamObserver<BalanceUpdate>> observers = balanceStreamsByUser.get(userId);
        if (observers == null || observers.isEmpty()) return;

        for (StreamObserver<BalanceUpdate> obs : observers) {
            try {
                obs.onNext(update);
            } catch (Exception e) {
                observers.remove(obs);
            }
        }
    }
}
