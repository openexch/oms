// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.*;
import com.zaxxer.hikari.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.DriverManager;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "OMS_PG_TEST_URL", matches = ".+")
class PostgresOrderRecoveryTest {
    private HikariDataSource ds;
    private String schema;
    private PostgresOrderRepository repo;
    private final String url = System.getenv("OMS_PG_TEST_URL");
    private final String user = System.getenv().getOrDefault("OMS_PG_TEST_USER", "postgres");

    @BeforeEach void start() throws Exception {
        schema = "recovery_test_" + UUID.randomUUID().toString().replace("-", "");
        try (var c = DriverManager.getConnection(url, user, ""); var s = c.createStatement()) {
            s.execute("CREATE SCHEMA " + schema);
        }
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(user);
        config.setSchema(schema); config.setMaximumPoolSize(4);
        ds = new HikariDataSource(config);
        for (String migration : List.of("V001__init_schema.sql", "V006__order_recovery_state.sql")) {
            try (var c = ds.getConnection(); var s = c.createStatement();
                 var in = getClass().getResourceAsStream("/db/migration/" + migration)) {
                s.execute(new String(Objects.requireNonNull(in, migration).readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        repo = new PostgresOrderRepository(ds);
    }
    @AfterEach void stop() throws Exception {
        if (ds != null) ds.close();
        try (var c = DriverManager.getConnection(url, user, ""); var s = c.createStatement()) {
            s.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }
    private OmsOrder order() {
        var o = new OmsOrder();
        o.setOmsOrderId(100); o.setUserId(7); o.setMarketId(1);
        o.setSide(OrderSide.SELL); o.setOrderType(OmsOrderType.ICEBERG);
        o.setTimeInForce(TimeInForce.GTC); o.setStatus(OmsOrderStatus.PARTIALLY_FILLED);
        o.setQuantity(100); o.setFilledQty(13); o.setRemainingQty(87);
        o.setDisplayQuantity(10); o.setHiddenQuantity(90); o.setSliceRemainingQty(7);
        o.setTrailingArmPrice(901); o.setCancelRequested(true);
        o.setHoldId(300); o.setHoldAmount(87); o.setParentOmsOrderId(99);
        o.setReplacePendingOldClusterOrderId(200); o.setPendingPrice(902);
        o.setPendingQuantity(120); o.setPendingHoldRequested(20); o.setPendingHoldDelta(20); o.setPendingHoldTarget(107);
        o.setReplaceRequestedAtMs(1234567);
        return o;
    }
    private void assertRecovery(OmsOrder before, OmsOrder after) {
        assertNotNull(after);
        assertAll(
            () -> assertEquals(before.getTrailingArmPrice(), after.getTrailingArmPrice(), "trailing extreme"),
            () -> assertEquals(before.getHiddenQuantity(), after.getHiddenQuantity(), "iceberg hidden"),
            () -> assertEquals(before.getSliceRemainingQty(), after.getSliceRemainingQty(), "iceberg slice"),
            () -> assertEquals(before.isCancelRequested(), after.isCancelRequested(), "cancel intent"),
            () -> assertEquals(before.getHoldId(), after.getHoldId(), "hold identity"),
            () -> assertEquals(before.getParentOmsOrderId(), after.getParentOmsOrderId(), "parent identity"),
            () -> assertEquals(before.getReplacePendingOldClusterOrderId(), after.getReplacePendingOldClusterOrderId(), "replace identity"),
            () -> assertEquals(before.getPendingPrice(), after.getPendingPrice()),
            () -> assertEquals(before.getPendingQuantity(), after.getPendingQuantity()),
            () -> assertEquals(before.getPendingHoldRequested(), after.getPendingHoldRequested()),
            () -> assertEquals(before.getPendingHoldDelta(), after.getPendingHoldDelta()),
            () -> assertEquals(before.getPendingHoldTarget(), after.getPendingHoldTarget()),
            () -> assertEquals(before.getReplaceRequestedAtMs(), after.getReplaceRequestedAtMs()));
    }
    @Test void restartAndAllReadPathsPreserveOutstandingWorkflow() {
        var before = order(); repo.saveOrder(before);
        var restarted = new PostgresOrderRepository(ds);
        assertRecovery(before, restarted.findById(100));
        assertRecovery(before, restarted.findAllOpenOrders().getFirst());
        assertRecovery(before, restarted.findOpenOrders(7).getFirst());
        assertRecovery(before, restarted.findByUser(7, 10, 0).getFirst());
        assertRecovery(before, restarted.findByUserAndStatus(7, before.getStatus(), 10, 0).getFirst());
    }
    @Test void workflowResolutionClearsPersistedMarkersAndPreservesPartialSlice() {
        var before = order(); repo.saveOrder(before);
        before.setCancelRequested(false); before.setReplacePendingOldClusterOrderId(0);
        before.setPendingPrice(0); before.setPendingQuantity(0); before.setPendingHoldDelta(0);
        before.setPendingHoldTarget(0); before.setReplaceRequestedAtMs(0);
        before.setHiddenQuantity(80); before.setSliceRemainingQty(3); before.setTrailingArmPrice(999);
        repo.saveOrder(before);
        assertRecovery(before, new PostgresOrderRepository(ds).findById(100));
    }
    @Test void staleDetachedSnapshotCannotOverwriteACommittedCancelIntent() {
        var before = order(); before.setCancelRequested(false); repo.saveOrder(before);
        var stale = repo.findById(100);
        before.setCancelRequested(true); repo.saveOrder(before);
        assertThrows(PersistenceException.class, () -> repo.saveOrder(stale));
        assertTrue(repo.findById(100).isCancelRequested());
    }
}
