// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

/**
 * The slice of {@link AssetsClusterClient} the balance store depends on — an interface so the
 * money-critical store logic (timeout compensator, projection consistency) unit-tests against a
 * fake transport with scripted ack behavior.
 */
public interface AssetsTransport {

    boolean submitHold(long correlationId, long orderId, long userId, int assetId, long amount, boolean omsManagedRelease);

    /** {@code amount < 0} releases the hold's full residual. */
    boolean submitRelease(long orderId, long userId, long amount);

    boolean submitDeposit(long correlationId, long userId, int assetId, long amount);

    boolean submitWithdraw(long correlationId, long userId, int assetId, long amount);

    boolean submitRequestBalanceSnapshot(long correlationId);

    boolean isConnected();

    void setEgressListener(AssetsEgressListener listener);
}
