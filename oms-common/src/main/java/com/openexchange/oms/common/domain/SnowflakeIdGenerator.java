// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.common.domain;

/**
 * Snowflake-style ID generator for OMS order IDs.
 * Generates unique, time-ordered 64-bit IDs.
 *
 * Layout (64 bits):
 *   - 1 bit: unused (sign)
 *   - 41 bits: millisecond timestamp (69 years from epoch)
 *   - 10 bits: node ID (0-1023)
 *   - 12 bits: sequence (0-4095 per ms per node)
 */
public class SnowflakeIdGenerator {

    private static final long EPOCH = 1704067200000L; // 2024-01-01T00:00:00Z
    private static final int NODE_BITS = 10;
    private static final int SEQUENCE_BITS = 12;
    private static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;
    private static final long MAX_NODE_ID = (1L << NODE_BITS) - 1;

    private final long nodeId;
    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long nodeId) {
        if (nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException("Node ID must be between 0 and " + MAX_NODE_ID);
        }
        this.nodeId = nodeId;
    }

    /**
     * Generate next unique ID. Thread-safe via synchronization.
     */
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                // Sequence exhausted for this millisecond, wait for next ms
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }

        lastTimestamp = timestamp;

        return ((timestamp - EPOCH) << (NODE_BITS + SEQUENCE_BITS))
             | (nodeId << SEQUENCE_BITS)
             | sequence;
    }

    /**
     * Recover the wall-clock millisecond timestamp an id was minted at, inverting the layout above
     * ({@code ts = (id >>> (NODE_BITS + SEQUENCE_BITS)) + EPOCH}). This is what lets the orphan-hold
     * reconciler age-gate a hold whose order the OMS has NO record of: the omsOrderId still carries
     * its own creation time. Only meaningful for ids from this generator; a value that decodes to a
     * timestamp before {@link #EPOCH} or absurdly far in the future is not a real Snowflake id and
     * callers should treat the age as unknown.
     *
     * @param id a Snowflake id from {@link #nextId()}
     * @return the epoch-millis creation time encoded in the id
     */
    public static long timestampMillis(long id) {
        return (id >>> (NODE_BITS + SEQUENCE_BITS)) + EPOCH;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
