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
            try(var abort=getClass().getResourceAsStream("/db/migration/V009__abort_unsent_me_commands.sql")) {
                s.execute(new String(Objects.requireNonNull(abort).readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));
            }
            s.execute("""
                CREATE TABLE me_command_outcomes(command_id_high bigint,command_id_low bigint,canonical_payload bytea,
                    order_id bigint NOT NULL DEFAULT 0,status int NOT NULL DEFAULT 0,reason int NOT NULL DEFAULT 0,
                    result int NOT NULL DEFAULT 0)""");
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
    private void project(DurableOrderIntent cmd) throws Exception { project(cmd,0,0,0,0); }
    private void project(DurableOrderIntent cmd,long orderId,int status,int reason,int result) throws Exception {
        byte[] wire=DurableCommandWire.encode(cmd);
        try(var c=ds.getConnection();var p=c.prepareStatement("""
                INSERT INTO me_command_outcomes(command_id_high,command_id_low,canonical_payload,order_id,status,reason,result)
                VALUES(?,?,?,?,?,?,?)""")) {
            p.setLong(1,cmd.idHigh());p.setLong(2,cmd.idLow());p.setBytes(3,Arrays.copyOfRange(wire,8,wire.length));
            p.setLong(4,orderId);p.setInt(5,status);p.setInt(6,reason);p.setInt(7,result);p.executeUpdate();
        }
    }
    @Test void abortedCommandIsNeverSendableOrReopened() {
        var cmd=command(1,1000);repo.prepare(cmd,1);repo.ready(cmd);
        repo.abortUnsent(cmd);
        assertTrue(repo.pending(10).isEmpty());
        assertTrue(repo.openCommands(Long.MIN_VALUE,Long.MIN_VALUE,10).isEmpty());
        assertThrows(PersistenceException.class,()->repo.ready(cmd));
        assertEquals("ABORTED",repo.prepare(cmd,1).state());
        repo.abortUnsent(cmd); // idempotent for the same payload
        assertThrows(PersistenceException.class,()->repo.abortUnsent(command(1,2000)));
    }
    @Test void resolvedCommandCannotBeAborted() throws Exception {
        var cmd=command(1,1000);repo.prepare(cmd,1);repo.ready(cmd);project(cmd);
        repo.resolve(cmd);
        assertThrows(PersistenceException.class,()->repo.abortUnsent(cmd));
    }
    @Test void openCommandsPageByIdentityAndIncludePreparedAndReady() {
        var a=command(1,1000);var b=new DurableOrderIntent(17,2,100,9002,0,1000,100,0,1,0,0,0);
        var c=new DurableOrderIntent(18,1,100,9003,0,1000,100,0,1,0,0,0);
        repo.prepare(a,1);repo.ready(a);repo.prepare(b,1);repo.prepare(c,1);repo.ready(c);
        var first=repo.openCommands(Long.MIN_VALUE,Long.MIN_VALUE,2);
        assertEquals(List.of(a,b),first.stream().map(PostgresCommandRepository.Entry::intent).toList());
        assertEquals(List.of("READY","PREPARED"),first.stream().map(PostgresCommandRepository.Entry::state).toList());
        var last=first.get(1).intent();
        assertEquals(List.of(c),repo.openCommands(last.idHigh(),last.idLow(),2).stream().map(PostgresCommandRepository.Entry::intent).toList());
        assertThrows(IllegalArgumentException.class,()->repo.openCommands(0,0,0));
    }
    @Test void projectedOutcomeCarriesCanonicalResultOnlyForExactPayload() throws Exception {
        var cmd=command(1,1000);repo.prepare(cmd,1);repo.ready(cmd);
        var other=new DurableOrderIntent(17,2,100,9002,0,1000,100,0,1,0,0,0);repo.prepare(other,1);repo.ready(other);
        project(cmd,77,0,0,0);project(command(2,5000),88,4,3,1); // same identity as other, different payload
        assertEquals(List.of(new DurableCommandStore.Outcome(cmd,77,0,0,0)),repo.projectedOutcomes(10));
        repo.resolve(cmd);
        assertTrue(repo.projectedOutcomes(10).isEmpty());
        repo.resolve(cmd); // idempotent once resolved
        assertThrows(PersistenceException.class,()->repo.resolve(other)); // no exact outcome: stays READY
        assertEquals(List.of(other),repo.pending(10).stream().map(PostgresCommandRepository.Entry::intent).toList());
    }
}
