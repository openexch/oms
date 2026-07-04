// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps the gRPC server lifecycle.
 */
public class GrpcServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private final int port;
    private final GrpcOrderService orderService;
    private final GrpcAccountService accountService;
    private final ServerInterceptor authInterceptor;
    private Server server;

    public GrpcServer(int port, GrpcOrderService orderService, GrpcAccountService accountService,
                      ServerInterceptor authInterceptor) {
        this.port = port;
        this.orderService = orderService;
        this.accountService = accountService;
        this.authInterceptor = authInterceptor;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(orderService)
                .addService(accountService)
                .intercept(authInterceptor)
                .build()
                .start();
        log.info("gRPC server started on port {}", port);
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
            log.info("gRPC server stopped");
        }
    }

    public GrpcOrderService getOrderService() {
        return orderService;
    }

    public GrpcAccountService getAccountService() {
        return accountService;
    }
}
