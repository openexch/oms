// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.cluster;

import com.match.infrastructure.generated.MessageHeaderDecoder;
import com.match.infrastructure.generated.MessageHeaderEncoder;
import com.match.infrastructure.generated.OrderSide;
import com.match.infrastructure.generated.OrderStatus;
import com.match.infrastructure.generated.OrderStatusBatchDecoder;
import com.match.infrastructure.generated.OrderStatusBatchEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the SBE v6 reject-reason decode path (match#75) that {@code ClusterClient} feeds to the
 * egress listener. Exercises the REAL generated {@code OrderStatusBatch} codecs plus
 * {@link ClusterClient#normalizeRejectReason(short)} so both the wire encoding and the null/sentinel
 * handling are covered end to end (there is no lighter-weight seam: dispatch itself needs a live
 * Aeron client, so we drive the decoder directly here).
 */
class OrderStatusBatchRejectReasonTest {

    /** Encode a 2-order batch: a FILLED order (reason NONE=0) and a REJECTED order (NO_LIQUIDITY=9). */
    private ExpandableArrayBuffer encodeV6Batch() {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        OrderStatusBatchEncoder enc = new OrderStatusBatchEncoder();
        enc.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        enc.marketId(7).timestamp(1_000L);
        OrderStatusBatchEncoder.OrdersEncoder orders = enc.ordersCount(2);
        orders.next()
                .orderId(1000L).userId(1L).status(OrderStatus.FILLED).price(100L)
                .remainingQty(0L).filledQty(50L).side(OrderSide.BID).timestamp(1L)
                .omsOrderId(11L).statusSeq(1L).rejectReason((short) 0); // NONE
        orders.next()
                .orderId(1001L).userId(1L).status(OrderStatus.REJECTED).price(0L)
                .remainingQty(0L).filledQty(0L).side(OrderSide.ASK).timestamp(2L)
                .omsOrderId(12L).statusSeq(2L).rejectReason((short) 9); // NO_LIQUIDITY
        return buffer;
    }

    @Test
    void v6BatchDeliversRawReasonCodes() {
        ExpandableArrayBuffer buffer = encodeV6Batch();

        OrderStatusBatchDecoder dec = new OrderStatusBatchDecoder();
        dec.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        List<Integer> reasons = new ArrayList<>();
        for (OrderStatusBatchDecoder.OrdersDecoder order : dec.orders()) {
            reasons.add(ClusterClient.normalizeRejectReason(order.rejectReason()));
        }
        // Raw values pass through unchanged; 0 (NONE) is NOT the sentinel, only 255 is.
        assertEquals(List.of(0, 9), reasons);
    }

    @Test
    void preV6ActingStreamYieldsSentinel() {
        // Same bytes, decoded as if the stream's header said v5 (an OMS built on v6 reading a
        // pre-v6 publisher). The generated accessor short-circuits to the null value (255) whenever
        // actingVersion < 6, which normalizeRejectReason maps to the -1 sentinel: exactly the wire
        // behavior for a genuinely absent field on an older group block length.
        ExpandableArrayBuffer buffer = encodeV6Batch();

        OrderStatusBatchDecoder dec = new OrderStatusBatchDecoder();
        dec.wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH,
                OrderStatusBatchDecoder.BLOCK_LENGTH, 5 /* actingVersion */);

        List<Integer> reasons = new ArrayList<>();
        for (OrderStatusBatchDecoder.OrdersDecoder order : dec.orders()) {
            reasons.add(ClusterClient.normalizeRejectReason(order.rejectReason()));
        }
        assertEquals(List.of(-1, -1), reasons);
    }
}
