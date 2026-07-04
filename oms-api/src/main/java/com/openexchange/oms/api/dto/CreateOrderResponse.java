// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * Response DTO for order creation.
 */
public class CreateOrderResponse {

    private boolean accepted;
    // Snowflake id as a JSON string on the wire (oms#39)
    @JsonSerialize(using = ToStringSerializer.class)
    private long omsOrderId;
    private String status;
    private String rejectReason;
    // True when this clientOrderId already names an active order (oms#40):
    // no new order was created; omsOrderId/status describe the existing one.
    private boolean duplicate;

    public static CreateOrderResponse accepted(long omsOrderId, String status) {
        CreateOrderResponse r = new CreateOrderResponse();
        r.accepted = true;
        r.omsOrderId = omsOrderId;
        r.status = status;
        return r;
    }

    public static CreateOrderResponse duplicate(long omsOrderId, String status) {
        CreateOrderResponse r = accepted(omsOrderId, status);
        r.duplicate = true;
        return r;
    }

    public static CreateOrderResponse rejected(String reason) {
        CreateOrderResponse r = new CreateOrderResponse();
        r.accepted = false;
        r.rejectReason = reason;
        r.status = "REJECTED";
        return r;
    }

    public boolean isAccepted() { return accepted; }
    public void setAccepted(boolean accepted) { this.accepted = accepted; }

    public long getOmsOrderId() { return omsOrderId; }
    public void setOmsOrderId(long omsOrderId) { this.omsOrderId = omsOrderId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getRejectReason() { return rejectReason; }
    public void setRejectReason(String rejectReason) { this.rejectReason = rejectReason; }

    public boolean isDuplicate() { return duplicate; }
    public void setDuplicate(boolean duplicate) { this.duplicate = duplicate; }
}
