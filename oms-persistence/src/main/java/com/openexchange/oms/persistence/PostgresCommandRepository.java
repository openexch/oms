// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.match.domain.commands.DurableOrderIntent;
import com.openexchange.oms.common.DurableCommandWire;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.*;

/** Durable command outbox. SQL callers run outside the Aeron polling thread. */
public final class PostgresCommandRepository {
    public record Entry(DurableOrderIntent intent,long revision,String state) {}
    private final HikariDataSource dataSource;
    public PostgresCommandRepository(HikariDataSource dataSource) { this.dataSource=dataSource; }
    public Entry prepare(DurableOrderIntent intent,long revision) {
        try(var c=dataSource.getConnection()) { return prepare(c,intent,revision); }
        catch(SQLException e) { throw new PersistenceException("Cannot prepare ME command",e); }
    }
    /** May share the order/workflow transaction; this method never commits caller-owned state. */
    public Entry prepare(Connection c,DurableOrderIntent intent,long revision) throws SQLException {
        if(revision<=0) throw new IllegalArgumentException("Workflow revision required");
        byte[] payload=DurableCommandWire.encode(intent);
        try(var p=c.prepareStatement("""
            INSERT INTO oms_me_commands(command_id_high,command_id_low,oms_order_id,workflow_revision,command_kind,payload)
            VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING
            """)) {
            p.setLong(1,intent.idHigh()); p.setLong(2,intent.idLow()); p.setLong(3,intent.omsOrderId());
            p.setLong(4,revision); p.setInt(5,intent.kind()); p.setBytes(6,payload); p.executeUpdate();
        }
        try(var p=c.prepareStatement("""
            SELECT payload,workflow_revision,state FROM oms_me_commands
            WHERE (command_id_high=? AND command_id_low=?) OR (oms_order_id=? AND workflow_revision=? AND command_kind=?)
            """)) {
            p.setLong(1,intent.idHigh());p.setLong(2,intent.idLow());p.setLong(3,intent.omsOrderId());p.setLong(4,revision);p.setInt(5,intent.kind());
            try(var rows=p.executeQuery()) {
                if(!rows.next() || !Arrays.equals(payload,rows.getBytes(1)) || rows.getLong(2)!=revision)
                    throw new SQLException("ME command identity/workflow conflict");
                var result=new Entry(intent,revision,rows.getString(3));
                if(rows.next()) throw new SQLException("Ambiguous ME command identity");
                return result;
            }
        }
    }
    /** Call only after the durable hold/workflow prerequisite is established. Never on a timeout. */
    public void ready(DurableOrderIntent intent) {
        try(var c=dataSource.getConnection(); var p=c.prepareStatement("""
            UPDATE oms_me_commands SET state=CASE WHEN state='PREPARED' THEN 'READY' ELSE state END
            WHERE command_id_high=? AND command_id_low=? AND payload=?
            """)) {
            p.setLong(1,intent.idHigh());p.setLong(2,intent.idLow());p.setBytes(3,DurableCommandWire.encode(intent));
            if(p.executeUpdate()!=1) throw new SQLException("Missing/conflicting prepared ME command");
        } catch(SQLException e) { throw new PersistenceException("Cannot ready ME command",e); }
    }
    public List<Entry> pending(int limit) {
        if(limit<1 || limit>256) throw new IllegalArgumentException("Invalid outbox batch bound");
        try(var c=dataSource.getConnection();var p=c.prepareStatement("""
            SELECT payload,workflow_revision,state FROM oms_me_commands WHERE state='READY'
            ORDER BY created_at,command_id_high,command_id_low LIMIT ?
            """)) {
            p.setInt(1,limit);var result=new ArrayList<Entry>();
            try(var rows=p.executeQuery()) { while(rows.next()) result.add(new Entry(DurableCommandWire.decode(rows.getBytes(1)),rows.getLong(2),rows.getString(3))); }
            return result;
        } catch(SQLException e) { throw new PersistenceException("Cannot read ME command outbox",e); }
    }
    /** Only the Archive projector's exact canonical result resolves send uncertainty. */
    public int resolveProjected() {
        try(var c=dataSource.getConnection();var p=c.prepareStatement("""
            UPDATE oms_me_commands c SET state='RESOLVED',resolved_at=NOW()
            FROM me_command_outcomes o
            WHERE c.state='READY' AND c.command_id_high=o.command_id_high AND c.command_id_low=o.command_id_low
              AND substring(c.payload FROM 9)=substring(o.canonical_payload FROM 1 FOR 71)
            """)) { return p.executeUpdate(); }
        catch(SQLException e) { throw new PersistenceException("Cannot resolve ME command outcomes",e); }
    }
}
