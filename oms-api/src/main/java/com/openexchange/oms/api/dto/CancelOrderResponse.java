// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

public class CancelOrderResponse {

    private boolean accepted;
    // Snowflake id as a JSON string on the wire (oms#39)
    @JsonSerialize(using = ToStringSerializer.class)
    private long omsOrderId;
    private String message;

    public static CancelOrderResponse accepted(long omsOrderId) {
        CancelOrderResponse r = new CancelOrderResponse();
        r.accepted = true;
        r.omsOrderId = omsOrderId;
        r.message = "Cancel request submitted";
        return r;
    }

    public static CancelOrderResponse rejected(String message) {
        CancelOrderResponse r = new CancelOrderResponse();
        r.accepted = false;
        r.message = message;
        return r;
    }

    public boolean isAccepted() { return accepted; }
    public void setAccepted(boolean accepted) { this.accepted = accepted; }

    public long getOmsOrderId() { return omsOrderId; }
    public void setOmsOrderId(long omsOrderId) { this.omsOrderId = omsOrderId; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
