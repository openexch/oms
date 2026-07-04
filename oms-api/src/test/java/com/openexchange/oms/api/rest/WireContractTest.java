// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openexchange.oms.api.dto.CancelOrderResponse;
import com.openexchange.oms.api.dto.CreateOrderRequest;
import com.openexchange.oms.api.dto.CreateOrderResponse;
import com.openexchange.oms.api.dto.OrderResponse;
import com.openexchange.oms.common.domain.OmsOrder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The frozen wire contract (oms#39 / P4.1): money crosses as exact 8-dp
 * decimal STRINGS, 64-bit Snowflake ids as JSON STRINGS. These tests pin the
 * serialized shapes; changing them is an API break (see docs/API.md SemVer
 * policy).
 */
class WireContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // A Snowflake-sized id beyond JS Number.MAX_SAFE_INTEGER (2^53-1);
    // trading-ui#25 saw ...544 become ...540 through a JSON number.
    private static final long BIG_ID = 330_211_482_313_099_544L;

    @Test
    void orderResponseSerializesMoneyAndIdsAsStrings() throws Exception {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(BIG_ID);
        order.setClusterOrderId(BIG_ID + 1);
        order.setUserId(7);
        order.setMarketId(1);
        order.setPrice(11_022_400_000_000L);      // 110224.00000000
        order.setQuantity(10_000_000L);           // 0.10000000 — inexact as a double
        order.setFilledQty(1L);                   // 0.00000001
        order.setRemainingQty(9_999_999L);
        order.setStopPrice(0L);

        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(OrderResponse.fromOrder(order)));

        assertTrue(json.get("omsOrderId").isTextual(), "omsOrderId must be a JSON string");
        assertEquals("330211482313099544", json.get("omsOrderId").textValue());
        assertEquals("330211482313099545", json.get("clusterOrderId").textValue());
        assertTrue(json.get("userId").isNumber(), "small userId stays a number");

        assertTrue(json.get("price").isTextual(), "money must be a JSON string");
        assertEquals("110224.00000000", json.get("price").textValue());
        assertEquals("0.10000000", json.get("quantity").textValue());
        assertEquals("0.00000001", json.get("filledQty").textValue());
        assertEquals("0.09999999", json.get("remainingQty").textValue());
        assertEquals("0.00000000", json.get("stopPrice").textValue());
    }

    @Test
    void createAndCancelResponsesSerializeIdAsString() throws Exception {
        JsonNode created = MAPPER.readTree(
                MAPPER.writeValueAsString(CreateOrderResponse.accepted(BIG_ID, "PENDING_NEW")));
        assertEquals("330211482313099544", created.get("omsOrderId").textValue());

        JsonNode cancelled = MAPPER.readTree(
                MAPPER.writeValueAsString(CancelOrderResponse.accepted(BIG_ID)));
        assertEquals("330211482313099544", cancelled.get("omsOrderId").textValue());
    }

    @Test
    void executionResponseSerializesMoneyAndIdsAsStrings() throws Exception {
        com.openexchange.oms.common.domain.ExecutionReport exec =
                new com.openexchange.oms.common.domain.ExecutionReport();
        exec.setTradeId(BIG_ID);
        exec.setOmsOrderId(BIG_ID + 1);
        exec.setUserId(7);
        exec.setMarketId(1);
        exec.setSide(com.openexchange.oms.common.enums.OrderSide.BUY);
        exec.setPrice(11_022_400_000_000L);       // 110224.00000000
        exec.setQuantity(10_000_001L);            // 0.10000001
        exec.setMaker(true);
        exec.setExecutedAtMs(1_700_000_000_000L);

        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(
                com.openexchange.oms.api.dto.ExecutionResponse.fromExecution(exec)));

        assertTrue(json.get("tradeId").isTextual(), "tradeId must be a JSON string");
        assertEquals("330211482313099544", json.get("tradeId").textValue());
        assertEquals("330211482313099545", json.get("omsOrderId").textValue());
        assertTrue(json.get("userId").isNumber(), "small userId stays a number");
        assertEquals("110224.00000000", json.get("price").textValue());
        assertEquals("0.10000001", json.get("quantity").textValue());
        assertTrue(json.get("maker").isBoolean());
        assertEquals(1_700_000_000_000L, json.get("executedAtMs").longValue());
    }

    @Test
    void positionResponseSerializesSignedNetQuantityAsString() throws Exception {
        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(
                new com.openexchange.oms.api.dto.PositionResponse(7, 1, -10_000_000L)));
        assertEquals("-0.10000000", json.get("netQuantity").textValue(),
                "net positions are signed decimal strings");
    }

    @Test
    void duplicateCreateResponseCarriesFlagAndExistingId() throws Exception {
        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(
                CreateOrderResponse.duplicate(BIG_ID, "NEW")));
        assertTrue(json.get("accepted").booleanValue());
        assertTrue(json.get("duplicate").booleanValue());
        assertEquals("330211482313099544", json.get("omsOrderId").textValue());
        assertEquals("NEW", json.get("status").textValue());
    }

    @Test
    void createOrderRequestParsesDecimalStringsExactly() throws Exception {
        CreateOrderRequest req = MAPPER.readValue(
                "{\"marketId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\","
                        + "\"price\":\"110224.00000001\",\"quantity\":\"0.1\"}",
                CreateOrderRequest.class);
        assertEquals(11_022_400_000_001L, req.getPrice());
        assertEquals(10_000_000L, req.getQuantity());
        assertEquals(0L, req.getStopPrice(), "absent money fields default to 0");
    }

    @Test
    void createOrderRequestStillAcceptsLegacyNumbers() throws Exception {
        CreateOrderRequest req = MAPPER.readValue(
                "{\"marketId\":1,\"price\":50000.5,\"quantity\":2}",
                CreateOrderRequest.class);
        assertEquals(5_000_050_000_000L, req.getPrice());
        assertEquals(200_000_000L, req.getQuantity());
    }

    @Test
    void createOrderRequestRejectsMalformedMoneyStrings() {
        for (String bad : new String[]{"\"1.123456789\"", "\"1e5\"", "\"abc\"", "\"\"", "true"}) {
            assertThrows(java.io.IOException.class, () -> MAPPER.readValue(
                    "{\"marketId\":1,\"price\":" + bad + ",\"quantity\":\"1\"}",
                    CreateOrderRequest.class), "should reject price=" + bad);
        }
    }
}
