package com.openexchange.oms.api.dto;

/**
 * Response DTO for order creation.
 */
public class CreateOrderResponse {

    private boolean accepted;
    private long omsOrderId;
    private String status;
    private String rejectReason;

    public static CreateOrderResponse accepted(long omsOrderId, String status) {
        CreateOrderResponse r = new CreateOrderResponse();
        r.accepted = true;
        r.omsOrderId = omsOrderId;
        r.status = status;
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
}
