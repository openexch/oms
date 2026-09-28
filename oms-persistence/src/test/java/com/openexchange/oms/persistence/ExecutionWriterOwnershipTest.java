// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "OMS_PG_TEST_URL", matches = ".+")
class ExecutionWriterOwnershipTest {
    private String schema;
    private Connection db;
    private final String url = System.getenv("OMS_PG_TEST_URL");
    private final String user = System.getenv().getOrDefault("OMS_PG_TEST_USER", "postgres");
    private void sql(Connection c, String text) throws SQLException {
        try (var s = c.createStatement()) { s.execute(text); }
    }
    private Connection connect() throws SQLException {
        var c = DriverManager.getConnection(url, user, "");
        sql(c, "SET search_path TO " + schema); return c;
    }
    @BeforeEach void setup() throws Exception {
        schema = "writer_test_" + UUID.randomUUID().toString().replace("-", "");
        db = DriverManager.getConnection(url, user, "");
        sql(db, "CREATE SCHEMA " + schema); sql(db, "SET search_path TO " + schema);
        sql(db, "CREATE TABLE executions(id bigint PRIMARY KEY)");
        try (var in = getClass().getResourceAsStream("/db/migration/V007__execution_writer_ownership.sql")) {
            sql(db, new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.UTF_8));
        }
    }
    @AfterEach void cleanup() throws Exception {
        if (db != null) { sql(db, "DROP SCHEMA " + schema + " CASCADE"); db.close(); }
    }
    @Test void legacyBinaryCannotWriteAfterArchiveHandoff() throws Exception {
        try (var old = connect()) {
            sql(old, "INSERT INTO executions VALUES(1)");
            sql(db, "UPDATE execution_writer_ownership SET owner='archive',epoch=2");
            assertThrows(SQLException.class, () -> sql(old, "INSERT INTO executions VALUES(2)"));
        }
    }
    @Test void pausedOwnershipRejectsEveryMutation() throws Exception {
        sql(db, "INSERT INTO executions VALUES(1)");
        sql(db, "UPDATE execution_writer_ownership SET owner='paused',epoch=2");
        assertThrows(SQLException.class, () -> sql(db, "INSERT INTO executions VALUES(2)"));
        assertThrows(SQLException.class, () -> sql(db, "UPDATE executions SET id=3"));
        assertThrows(SQLException.class, () -> sql(db, "DELETE FROM executions"));
        assertThrows(SQLException.class, () -> sql(db, "TRUNCATE executions"));
    }
    @Test void oldArchiveEpochCannotResumeAfterPauseAndReactivation() throws Exception {
        sql(db, "UPDATE execution_writer_ownership SET owner='archive',epoch=2");
        try (var worker = connect()) {
            sql(worker, "SET oe.execution_writer='archive'; SET oe.execution_writer_epoch='2'");
            sql(worker, "INSERT INTO executions VALUES(1)");
            sql(db, "UPDATE execution_writer_ownership SET owner='archive',epoch=4");
            assertThrows(SQLException.class, () -> sql(worker, "INSERT INTO executions VALUES(2)"));
        }
    }
    @Test void handoffCannotOvertakeAnUncommittedLegacyWrite() throws Exception {
        try (var writer = connect(); var controller = connect()) {
            writer.setAutoCommit(false);
            sql(writer, "INSERT INTO executions VALUES(1)");
            sql(controller, "SET lock_timeout='250ms'");
            var blocked = assertThrows(SQLException.class, () -> sql(controller,
                    "SELECT transition_execution_writer('legacy',1,'paused','test drain')"));
            assertEquals("55P03", blocked.getSQLState());
            writer.commit();
            sql(controller, "SELECT transition_execution_writer('legacy',1,'paused','test drained')");
            assertThrows(SQLException.class, () -> sql(writer, "INSERT INTO executions VALUES(2)"));
            writer.rollback();
        }
    }
    @Test void handoffRequiresExpectedEpochAndPausedBoundaryAndRecordsRollback() throws Exception {
        assertThrows(SQLException.class, () -> sql(db,
                "SELECT transition_execution_writer('legacy',1,'archive','unsafe shortcut')"));
        sql(db, "SELECT transition_execution_writer('legacy',1,'paused','test stop legacy')");
        assertThrows(SQLException.class, () -> sql(db,
                "SELECT transition_execution_writer('paused',1,'archive','stale operator')"));
        sql(db, "SELECT transition_execution_writer('paused',2,'archive','test activation')");
        sql(db, "SELECT transition_execution_writer('archive',3,'paused','test stop worker')");
        sql(db, "SELECT transition_execution_writer('paused',4,'legacy','verified test rollback')");
        sql(db, "INSERT INTO executions VALUES(1)");
        try (var st = db.createStatement(); var rs = st.executeQuery("SELECT count(*) FROM execution_writer_handoffs")) {
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
        }
    }
    @Test void legacyStartupRefusesArchivedOwnership() throws Exception {
        var config = new com.zaxxer.hikari.HikariConfig();
        config.setJdbcUrl(url); config.setUsername(user); config.setSchema(schema); config.setMaximumPoolSize(1);
        try (var ds = new com.zaxxer.hikari.HikariDataSource(config)) {
            var repo = new PostgresExecutionRepository(ds);
            repo.requireLegacyWriterOwnership();
            sql(db, "SELECT transition_execution_writer('legacy',1,'paused','test startup guard')");
            assertThrows(PersistenceException.class, repo::requireLegacyWriterOwnership);
            sql(db, "SELECT transition_execution_writer('paused',2,'archive','test activation')");
            assertThrows(PersistenceException.class, repo::requireLegacyWriterOwnership);
        }
    }

}
