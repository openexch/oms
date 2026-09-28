// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.match.domain.commands.DurableOrderIntent;
import com.openexchange.oms.common.DurableCommandWire;
import com.zaxxer.hikari.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="OMS_PG_TEST_URL",matches=".+")
class PostgresCommandRepositoryTest {
    private HikariDataSource ds;
    private String schema;
    private PostgresCommandRepository repo;
    private final String url=System.getenv("OMS_PG_TEST_URL");
    private final String user=System.getenv().getOrDefault("OMS_PG_TEST_USER","postgres");
    @BeforeEach void start() throws Exception {
        schema="command_test_"+UUID.randomUUID().toString().replace("-","");
        try(var c=DriverManager.getConnection(url,user,"");var s=c.createStatement()) { s.execute("CREATE SCHEMA "+schema); }
        var cfg=new HikariConfig();cfg.setJdbcUrl(url);cfg.setUsername(user);cfg.setSchema(schema);cfg.setMaximumPoolSize(8);
        ds=new HikariDataSource(cfg);
        try(var c=ds.getConnection();var s=c.createStatement();var in=getClass().getResourceAsStream("/db/migration/V008__durable_me_commands.sql")) {
            s.execute(new String(Objects.requireNonNull(in).readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));
            s.execute("CREATE TABLE me_command_outcomes(command_id_high bigint,command_id_low bigint,canonical_payload bytea)");
        }
        repo=new PostgresCommandRepository(ds);
    }
    @AfterEach void stop() throws Exception {
        if(ds!=null) ds.close();
        try(var c=DriverManager.getConnection(url,user,"");var s=c.createStatement()) { s.execute("DROP SCHEMA "+schema+" CASCADE"); }
    }
    private DurableOrderIntent command(long id,long price) { return new DurableOrderIntent(17,id,100,9001,0,price,100,0,1,0,0,0); }
    @Test void preparedIntentCannotBeSentAndReadyRetryKeepsIdentityAcrossRestart() {
        var cmd=command(1,1000);repo.prepare(cmd,1);
        assertTrue(repo.pending(10).isEmpty());repo.ready(cmd);
        var restarted=new PostgresCommandRepository(ds);
        assertEquals(List.of(new PostgresCommandRepository.Entry(cmd,1,"READY")),restarted.pending(10));
        assertEquals("READY",restarted.prepare(cmd,1).state());
        restarted.ready(cmd);assertEquals(1,restarted.pending(10).size());
    }
    @Test void conflictingIdentityAndNewIdForSameWorkflowAreRejected() {
        var cmd=command(1,1000);repo.prepare(cmd,1);
        assertThrows(PersistenceException.class,()->repo.prepare(command(1,2000),1));
        assertThrows(PersistenceException.class,()->repo.prepare(command(2,1000),1));
        assertThrows(PersistenceException.class,()->repo.ready(command(1,2000)));
        assertTrue(repo.pending(10).isEmpty());
    }
    @Test void failedCallerTransactionCannotLeaveSendableIntent() throws Exception {
        var cmd=command(1,1000);
        try(var c=ds.getConnection()) { c.setAutoCommit(false);repo.prepare(c,cmd,1);c.rollback(); }
        assertThrows(PersistenceException.class,()->repo.ready(cmd));assertTrue(repo.pending(10).isEmpty());
    }
    @Test void concurrentRetryCreatesOnePayload() throws Exception {
        var barrier=new CyclicBarrier(8);var result=new ArrayList<Future<PostgresCommandRepository.Entry>>();
        var cmd=command(1,1000);
        try(var pool=Executors.newFixedThreadPool(8)) {
            for(int i=0;i<8;i++) result.add(pool.submit(()->{barrier.await();return repo.prepare(cmd,1);}));
            for(var f:result) assertEquals(cmd,f.get(10,TimeUnit.SECONDS).intent());
        }
        repo.ready(cmd);assertEquals(1,repo.pending(10).size());
    }
    @Test void onlyExactProjectedOutcomeResolvesUncertainty() throws Exception {
        var cmd=command(1,1000);repo.prepare(cmd,1);repo.ready(cmd);
        assertEquals(0,repo.resolveProjected());
        project(command(1,2000));assertEquals(0,repo.resolveProjected());
        try(var c=ds.getConnection();var s=c.createStatement()) { s.execute("DELETE FROM me_command_outcomes"); }
        project(cmd);assertEquals(1,repo.resolveProjected());assertTrue(repo.pending(10).isEmpty());
        repo.ready(cmd);assertTrue(repo.pending(10).isEmpty()); // never re-open resolved send
        assertEquals(0,repo.resolveProjected());
    }
    private void project(DurableOrderIntent cmd) throws Exception {
        byte[] wire=DurableCommandWire.encode(cmd);
        try(var c=ds.getConnection();var p=c.prepareStatement("INSERT INTO me_command_outcomes VALUES(?,?,?)")) {
            p.setLong(1,cmd.idHigh());p.setLong(2,cmd.idLow());p.setBytes(3,Arrays.copyOfRange(wire,8,wire.length));p.executeUpdate();
        }
    }
}
