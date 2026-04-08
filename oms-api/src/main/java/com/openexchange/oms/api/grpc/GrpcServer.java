package com.openexchange.oms.api.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
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
    private Server server;

    public GrpcServer(int port, GrpcOrderService orderService, GrpcAccountService accountService) {
        this.port = port;
        this.orderService = orderService;
        this.accountService = accountService;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(orderService)
                .addService(accountService)
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
