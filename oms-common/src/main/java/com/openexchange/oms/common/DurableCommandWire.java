// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.common;

import com.match.domain.commands.DurableOrderIntent;
import com.match.infrastructure.generated.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** One exact, versioned payload for durable storage and every ingress retry. */
public final class DurableCommandWire {
    private DurableCommandWire() {}
    public static int encode(DurableOrderIntent c, MutableDirectBuffer b) {
        var e=new DurableOrderCommandEncoder().wrapAndApplyHeader(b,0,new MessageHeaderEncoder());
        e.commandIdHigh(c.idHigh()).commandIdLow(c.idLow()).userId(c.userId()).omsOrderId(c.omsOrderId())
            .oldOrderId(c.oldOrderId()).price(c.price()).quantity(c.quantity()).budget(c.budget()).marketId(c.marketId())
            .commandKind((short)c.kind()).orderType((short)c.type()).orderSide((short)c.side());
        return 8+e.encodedLength();
    }
    public static byte[] encode(DurableOrderIntent c) {
        byte[] bytes=new byte[8+DurableOrderCommandEncoder.BLOCK_LENGTH];
        encode(c,new UnsafeBuffer(bytes)); return bytes;
    }
    public static DurableOrderIntent decode(byte[] bytes) {
        if(bytes.length!=8+DurableOrderCommandEncoder.BLOCK_LENGTH) throw new IllegalStateException("Corrupt stored command length");
        var b=new UnsafeBuffer(bytes); var h=new MessageHeaderDecoder().wrap(b,0);
        if(h.schemaId()!=1 || h.version()!=11 || h.templateId()!=9 || h.blockLength()!=DurableOrderCommandEncoder.BLOCK_LENGTH)
            throw new IllegalStateException("Unsupported stored command wire");
        var d=new DurableOrderCommandDecoder().wrapAndApplyHeader(b,0,h);
        return new DurableOrderIntent(d.commandIdHigh(),d.commandIdLow(),d.userId(),d.omsOrderId(),d.oldOrderId(),
            d.price(),d.quantity(),d.budget(),d.marketId(),d.commandKind(),d.orderType(),d.orderSide());
    }
}
