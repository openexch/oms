package com.openexchange.oms.api.auth;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Authenticates every gRPC call and exposes the {@link Principal} via
 * {@link #PRINCIPAL} in the call context. Failures close the call with
 * UNAUTHENTICATED before it reaches a service method.
 */
public final class GrpcAuthInterceptor implements ServerInterceptor {

    public static final Context.Key<Principal> PRINCIPAL = Context.key("authPrincipal");

    private final AuthenticationProvider provider;

    public GrpcAuthInterceptor(AuthenticationProvider provider) {
        this.provider = provider;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        AuthenticationProvider.Headers accessor = name ->
                headers.get(Metadata.Key.of(name.toLowerCase(), Metadata.ASCII_STRING_MARSHALLER));
        try {
            Principal principal = provider.authenticate(accessor);
            Context context = Context.current().withValue(PRINCIPAL, principal);
            return Contexts.interceptCall(context, call, headers, next);
        } catch (AuthenticationException e) {
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }
}
