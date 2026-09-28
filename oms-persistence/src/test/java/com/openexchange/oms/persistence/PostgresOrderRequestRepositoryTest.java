// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "OMS_PG_TEST_URL", matches = ".+")
class PostgresOrderRequestRepositoryTest {
    private HikariDataSource ds;
    private String schema;
    private PostgresOrderRequestRepository repo;
    private final String url = System.getenv("OMS_PG_TEST_URL");
    private final String user = System.getenv().getOrDefault("OMS_PG_TEST_USER", "postgres");

    @BeforeEach void start() throws Exception {
        schema = "request_test_" + UUID.randomUUID().toString().replace("-", "");
        try (var c = DriverManager.getConnection(url, user, ""); var s = c.createStatement()) {
            s.execute("CREATE SCHEMA " + schema);
        }
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(user);
        config.setSchema(schema); config.setMaximumPoolSize(8);
        ds = new HikariDataSource(config);
        try (var c = ds.getConnection(); var s = c.createStatement();
             var in = getClass().getResourceAsStream("/db/migration/V005__durable_order_requests.sql")) {
            s.execute(new String(Objects.requireNonNull(in).readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
        repo = new PostgresOrderRequestRepository(ds);
    }
    @AfterEach void stop() throws Exception {
        if (ds != null) ds.close();
        try (var c = DriverManager.getConnection(url, user, ""); var s = c.createStatement()) {
            s.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }
    @Test void concurrentClaimHasExactlyOneOwnerAndOneDurableOrderId() throws Exception {
        var barrier = new CyclicBarrier(8);
        var claims = new ArrayList<Future<PostgresOrderRequestRepository.Claim>>();
        try (var pool = Executors.newFixedThreadPool(8)) {
            for (int i = 0; i < 8; i++) {
                long id = i + 10;
                claims.add(pool.submit(() -> { barrier.await(5, TimeUnit.SECONDS); return repo.claim(1, "req", "a".repeat(64), id); }));
            }
            var results = new ArrayList<PostgresOrderRequestRepository.Claim>();
            for (var future : claims) results.add(future.get(10, TimeUnit.SECONDS));
            assertEquals(1, results.stream().filter(PostgresOrderRequestRepository.Claim::created).count());
            assertEquals(1, results.stream().map(c -> c.entry().omsOrderId()).distinct().count());
        }
        assertFalse(new PostgresOrderRequestRepository(ds).find(1, "req").complete());
    }
    @Test void committedResponseSurvivesNewRepositoryAndCannotBeChanged() {
        String hash = "b".repeat(64);
        assertTrue(repo.claim(1, "req", hash, 123).created());
        var result = new PostgresOrderRequestRepository.Entry(123, hash, true, "PENDING_NEW", null);
        repo.complete(1, "req", result);
        var restarted = new PostgresOrderRequestRepository(ds);
        assertEquals(result, restarted.find(1, "req"));
        assertFalse(restarted.claim(1, "req", "c".repeat(64), 456).created());
        restarted.complete(1, "req", result); // Unknown COMMIT acknowledgement retry is idempotent.
        assertThrows(PersistenceException.class, () -> restarted.complete(1, "req",
                new PostgresOrderRequestRepository.Entry(456, hash, true, "PENDING_NEW", null)));
        assertEquals(result, restarted.find(1, "req"));
    }
    @Test void userScopeAndRejectedResponseAreDurable() {
        String hash = "d".repeat(64);
        repo.claim(1, "req", hash, 123); repo.claim(2, "req", hash, 456);
        repo.complete(1, "req", new PostgresOrderRequestRepository.Entry(0, hash, false, "REJECTED", "risk"));
        assertFalse(repo.find(1, "req").accepted());
        assertEquals(456, repo.find(2, "req").omsOrderId());
        assertFalse(repo.find(2, "req").complete());
    }
}
