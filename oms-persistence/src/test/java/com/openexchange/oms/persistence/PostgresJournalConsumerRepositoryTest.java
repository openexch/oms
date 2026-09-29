// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.*;
import com.openexchange.oms.persistence.JournalConsumerStore.*;
import com.zaxxer.hikari.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.DriverManager;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "OMS_PG_TEST_URL", matches = ".+")
class PostgresJournalConsumerRepositoryTest {
    private HikariDataSource ds;
    private String schema;
    private PostgresJournalConsumerRepository repo;
    private PostgresOrderRepository orders;
    private final String url = System.getenv("OMS_PG_TEST_URL");
    private final String user = System.getenv().getOrDefault("OMS_PG_TEST_USER", "postgres");

    @BeforeEach void start() throws Exception {
        schema = "journal_consumer_test_" + UUID.randomUUID().toString().replace("-", "");
        try (var c = DriverManager.getConnection(url, user, ""); var s = c.createStatement()) {
            s.execute("CREATE SCHEMA " + schema);
        }
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(user);
        config.setSchema(schema); config.setMaximumPoolSize(4);
        ds = new HikariDataSource(config);
        for (String migration : List.of("V001__init_schema.sql", "V006__order_recovery_state.sql",
                "V010__oms_journal_consumer.sql")) {
            try (var c = ds.getConnection(); var s = c.createStatement();
                 var in = getClass().getResourceAsStream("/db/migration/" + migration)) {
                s.execute(new String(Objects.requireNonNull(in, migration).readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        // The projector owns these tables; use its real schema, not a copy.
        try (var c = ds.getConnection(); var s = c.createStatement()) {
            s.execute(Files.readString(Path.of("../execution-projector/schema/projector.sql")));
        }
        repo = new PostgresJournalConsumerRepository(ds);
        orders = new PostgresOrderRepository(ds);
    }

    @AfterEach void stop() throws Exception {
        if (ds != null) ds.close();
        try (var c = DriverManager.getConnection(url, user, ""); var s = c.createStatement()) {
            s.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    private OmsOrder order(long id) {
        var o = new OmsOrder();
        o.setOmsOrderId(id); o.setUserId(7); o.setMarketId(1);
        o.setSide(OrderSide.BUY); o.setOrderType(OmsOrderType.LIMIT);
        o.setTimeInForce(TimeInForce.GTC); o.setStatus(OmsOrderStatus.NEW);
        o.setPrice(1000); o.setQuantity(100); o.setRemainingQty(100);
        return o;
    }

    private void sql(String statement) throws Exception {
        try (var c = ds.getConnection(); var s = c.createStatement()) { s.execute(statement); }
    }

    @Test void freshInstallationHasNoCheckpointAndNoOrders() {
        assertTrue(repo.loadCheckpoint().isEmpty());
        assertTrue(repo.projectorStatus().isEmpty());
        assertFalse(repo.hasOrders());
        assertTrue(repo.loadPositions().isEmpty());
        orders.saveOrder(order(1));
        assertTrue(repo.hasOrders());
    }

    @Test void projectorCheckpointAndEventsAreReadInJournalOrder() throws Exception {
        sql("INSERT INTO execution_projector_checkpoint(consumer,source_identity,recording_descriptor,position,last_trade_id)"
                + " VALUES('executions','me0-gen1','rec-7',192,2)");
        sql("INSERT INTO execution_journal_events VALUES('me0-gen1',96,1,'\\x01'),('me0-gen1',32,1,'\\x00'),"
                + "('me0-gen1',192,2,'\\x02'),('other',64,1,'\\x09')");
        assertEquals(new Checkpoint("me0-gen1", 192, 2), repo.projectorStatus().orElseThrow().checkpoint());
        var events = repo.eventsAfter("me0-gen1", 32, 10);
        assertEquals(List.of(96L, 192L), events.stream().map(Event::position).toList());
        assertEquals(1, events.get(0).templateId());
        assertArrayEquals(new byte[]{1}, events.get(0).payload());
        assertEquals(1, repo.eventsAfter("me0-gen1", 0, 1).size());
        assertThrows(IllegalArgumentException.class, () -> repo.eventsAfter("me0-gen1", 0, 0));
    }

    @Test void commitMovesOrdersPositionsAndCheckpointTogether() {
        var a = order(1); orders.saveOrder(a);
        var b = order(2); orders.saveOrder(b);
        long ra = a.getStateRevision(), rb = b.getStateRevision();
        a.setFilledQty(40); a.setRemainingQty(60); a.setStatus(OmsOrderStatus.PARTIALLY_FILLED);
        var first = new Checkpoint("me0-gen1", 96, 1);
        repo.commit(List.of(a, b), Map.of(new PositionKey(7, 1), 40L, new PositionKey(8, 1), -40L), Optional.empty(), first);
        assertEquals(ra + 1, a.getStateRevision());
        assertEquals(rb + 1, b.getStateRevision());
        assertEquals(40, orders.findById(1).getFilledQty());
        assertEquals(Optional.of(first), repo.loadCheckpoint());
        var second = new Checkpoint("me0-gen1", 160, 2);
        repo.commit(List.of(), Map.of(new PositionKey(7, 1), 10L), Optional.of(first), second);
        assertEquals(Map.of(new PositionKey(7, 1), 50L, new PositionKey(8, 1), -40L), repo.loadPositions());
        assertEquals(Optional.of(second), repo.loadCheckpoint());
    }

    @Test void staleOrderRevisionRollsBackTheWholeBatch() {
        var a = order(1); orders.saveOrder(a);
        var stale = order(1); stale.setStateRevision(a.getStateRevision() - 1); stale.setFilledQty(99);
        var cp = new Checkpoint("me0-gen1", 96, 1);
        assertThrows(PersistenceException.class,
                () -> repo.commit(List.of(stale), Map.of(new PositionKey(7, 1), 99L), Optional.empty(), cp));
        assertTrue(repo.loadCheckpoint().isEmpty());
        assertTrue(repo.loadPositions().isEmpty());
        assertEquals(0, orders.findById(1).getFilledQty());
        assertEquals(a.getStateRevision() - 1, stale.getStateRevision(), "failed batch must not advance revisions");
    }

    @Test void checkpointMovesOnlyFromTheExpectedPosition() {
        var first = new Checkpoint("me0-gen1", 96, 1);
        repo.commit(List.of(), Map.of(), Optional.empty(), first);
        assertThrows(PersistenceException.class,
                () -> repo.commit(List.of(), Map.of(), Optional.empty(), new Checkpoint("me0-gen1", 128, 1)));
        assertThrows(PersistenceException.class, () -> repo.commit(List.of(), Map.of(),
                Optional.of(new Checkpoint("me0-gen1", 64, 1)), new Checkpoint("me0-gen1", 128, 1)));
        assertThrows(IllegalArgumentException.class, () -> repo.commit(List.of(), Map.of(),
                Optional.of(first), new Checkpoint("me0-gen1", 64, 1)));
        assertThrows(IllegalArgumentException.class, () -> repo.commit(List.of(), Map.of(),
                Optional.of(first), new Checkpoint("me1-gen1", 128, 1)));
        assertEquals(Optional.of(first), repo.loadCheckpoint());
    }

    @Test void projectorStatusCarriesObservedTargetAndDatabaseClockFreshness() throws Exception {
        assertTrue(repo.projectorStatus().isEmpty());
        sql("INSERT INTO execution_projector_checkpoint(consumer,source_identity,recording_descriptor,position,last_trade_id)"
                + " VALUES('executions','me0-gen1','rec-7',192,2)");
        var never = repo.projectorStatus().orElseThrow();
        assertFalse(never.fresh(), "a worker that never observed is not fresh");
        sql("UPDATE execution_projector_checkpoint SET observed_target=4096, observed_at=NOW()");
        assertEquals(new ProjectorStatus(new Checkpoint("me0-gen1", 192, 2), 4096, true), repo.projectorStatus().orElseThrow());
        sql("UPDATE execution_projector_checkpoint SET observed_at=NOW() - interval '10 seconds'");
        assertFalse(repo.projectorStatus().orElseThrow().fresh());
    }
}
