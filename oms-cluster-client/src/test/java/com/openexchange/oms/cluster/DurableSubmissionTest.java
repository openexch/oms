// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.cluster;
import com.match.domain.commands.DurableOrderIntent;
import com.openexchange.oms.common.DurableCommandWire;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class DurableSubmissionTest {
    @Test void sameImmutablePayloadSurvivesEverySubmissionKind() throws Exception {
        for(int kind=0;kind<3;kind++) {
            var intent=new DurableOrderIntent(17,kind+1,100,9001,kind==0?0:15,1000,100,50,1,kind,0,0);
            var submission=OrderSubmission.durable(intent);
            assertSame(intent,submission.getDurableIntent());
            assertEquals(OrderSubmission.Type.values()[kind],submission.getType());
            assertEquals(intent,DurableCommandWire.decode(DurableCommandWire.encode(intent)));
            // Exercise the actual ClusterClient encoding path without opening a transport.
            var client=new ClusterClient();
            var method=ClusterClient.class.getDeclaredMethod("encodeSubmission",OrderSubmission.class);method.setAccessible(true);
            int size=(int)method.invoke(client,submission);
            var field=ClusterClient.class.getDeclaredField("encodeBuffer");field.setAccessible(true);
            var b=(org.agrona.DirectBuffer)field.get(client);byte[] actual=new byte[size];b.getBytes(0,actual);
            assertArrayEquals(DurableCommandWire.encode(intent),actual);
        }
    }
    @Test void storedWireTruncationAndHeaderSkewCannotBeRetried() {
        var intent=new DurableOrderIntent(17,1,100,9001,0,1000,100,0,1,0,0,0);
        byte[] bytes=DurableCommandWire.encode(intent);
        for(int n=0;n<bytes.length;n++) {
            final int size=n;assertThrows(IllegalStateException.class,()->DurableCommandWire.decode(java.util.Arrays.copyOf(bytes,size)));
        }
        bytes[6]=10;assertThrows(IllegalStateException.class,()->DurableCommandWire.decode(bytes));
    }
    @Test void hardOfferFailureReconnectsAndRetriesExactSameCommandBeforeLaterWork() {
        var client=new ClusterClient();
        var first=new DurableOrderIntent(17,1,100,9001,0,1000,100,0,1,0,0,0);
        var second=new DurableOrderIntent(17,2,100,9002,0,2000,100,0,1,0,0,0);
        assertTrue(client.submitOrder(OrderSubmission.durable(first)));
        assertTrue(client.submitOrder(OrderSubmission.durable(second)));
        var observed=new java.util.ArrayList<DurableOrderIntent>();
        var reconnects=new java.util.concurrent.atomic.AtomicInteger();
        assertEquals(0,client.drainOrderQueue((b,o,n)->{
            byte[] bytes=new byte[n];b.getBytes(o,bytes);observed.add(DurableCommandWire.decode(bytes));
            return io.aeron.Publication.MAX_POSITION_EXCEEDED;
        },reconnects::incrementAndGet));
        assertEquals(1,reconnects.get());assertEquals(java.util.List.of(first),observed);
        assertEquals(2,client.drainOrderQueue((b,o,n)->{
            byte[] bytes=new byte[n];b.getBytes(o,bytes);observed.add(DurableCommandWire.decode(bytes));return 128;
        },()->fail("unexpected reconnect")));
        assertEquals(java.util.List.of(first,first,second),observed);
    }

}
