package com.openexchange.oms.api.grpc;

import com.openexchange.oms.api.auth.Authorizer;
import com.openexchange.oms.api.auth.GrpcAuthInterceptor;
import com.openexchange.oms.api.auth.Principal;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** Per-call identity checks for gRPC service methods. */
final class GrpcAuth {

    private GrpcAuth() {}

    /**
     * Resolve the userId to act as (principal default when the request carries
     * none) and enforce authorization. On denial errors the observer with
     * PERMISSION_DENIED and returns null.
     */
    static Long resolveUserId(Authorizer authorizer, long requestedUserId, StreamObserver<?> responseObserver) {
        Principal principal = GrpcAuthInterceptor.PRINCIPAL.get();
        if (principal == null) {
            responseObserver.onError(Status.UNAUTHENTICATED
                    .withDescription("No authenticated principal").asRuntimeException());
            return null;
        }
        long userId = principal.resolveUserId(requestedUserId);
        if (!authorizer.allow(principal, Authorizer.ACTION_ACT_AS_USER, Long.toString(userId))) {
            responseObserver.onError(Status.PERMISSION_DENIED
                    .withDescription("Cannot act as user " + userId).asRuntimeException());
            return null;
        }
        return userId;
    }

    /** Whether the calling principal may act as the given user (no error emission). */
    static boolean canActAs(Authorizer authorizer, long userId) {
        Principal principal = GrpcAuthInterceptor.PRINCIPAL.get();
        return principal != null
                && authorizer.allow(principal, Authorizer.ACTION_ACT_AS_USER, Long.toString(userId));
    }
}
