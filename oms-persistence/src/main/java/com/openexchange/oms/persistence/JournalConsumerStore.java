// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.openexchange.oms.common.domain.OmsOrder;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The OMS side of the execution writer cutover: reads the events the C++ projector committed and
 * commits the OMS state derived from them together with the OMS's own checkpoint.
 */
public interface JournalConsumerStore {
    /** Where the OMS has applied up to. {@code lastTradeId} guards trade density across replays. */
    record Checkpoint(String source, long position, long lastTradeId) {}

    /** One projector-committed journal event: raw SBE message including its header. */
    record Event(long position, int templateId, byte[] payload) {}

    record PositionKey(long userId, int marketId) {}

    /** Absent only before the first commit of a fresh installation. */
    Optional<Checkpoint> loadCheckpoint();

    /** The projector's committed checkpoint, or empty before the projector has committed anything. */
    Optional<Checkpoint> projectorCheckpoint();

    /** Events of {@code source} strictly after {@code position}, in journal order. */
    List<Event> eventsAfter(String source, long position, int limit);

    /** Net positions as of the OMS checkpoint. */
    Map<PositionKey, Long> loadPositions();

    /** True when any order row exists: a missing checkpoint is then corruption, not a fresh install. */
    boolean hasOrders();

    /**
     * One transaction: every order row (revision compare-and-set), the position deltas and the
     * checkpoint move from {@code expected} (empty on the first commit) to {@code next}. On success the
     * orders' in-memory revisions advance. Any failure leaves all of it uncommitted.
     */
    void commit(Collection<OmsOrder> orders, Map<PositionKey, Long> positionDeltas,
                Optional<Checkpoint> expected, Checkpoint next);
}
