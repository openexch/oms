package com.openexchange.oms.persistence;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.openexchange.oms.common.domain.ExecutionReport;
import com.openexchange.oms.common.domain.OmsOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Async batch writer that consumes persistence events from an LMAX Disruptor ring buffer
 * and flushes them to PostgreSQL in configurable batches.
 *
 * <p>Supports three event types: orders, executions, and ledger entries.
 * Events are accumulated until the batch size is reached or the ring buffer is drained
 * (end of batch signalled by Disruptor), at which point they are flushed to the database.
 *
 * <p>Thread model: multiple producer threads publish events; a single consumer thread
 * drains and batches writes to avoid contention on the database connection pool.
 */
public class PersistenceBatchWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PersistenceBatchWriter.class);

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_RING_BUFFER_SIZE = 8192;

    private final PostgresOrderRepository orderRepo;
    private final PostgresExecutionRepository executionRepo;
    private final PostgresLedgerRepository ledgerRepo;
    private final int batchSize;

    private final Disruptor<PersistenceEvent> disruptor;
    private final RingBuffer<PersistenceEvent> ringBuffer;

    /**
     * Creates a batch writer with default batch size (100) and ring buffer size (8192).
     */
    public PersistenceBatchWriter(PostgresOrderRepository orderRepo,
                                  PostgresExecutionRepository executionRepo,
                                  PostgresLedgerRepository ledgerRepo) {
        this(orderRepo, executionRepo, ledgerRepo, DEFAULT_BATCH_SIZE, DEFAULT_RING_BUFFER_SIZE);
    }

    /**
     * Creates a batch writer with custom batch and ring buffer sizes.
     *
     * @param batchSize      number of events to accumulate before flushing
     * @param ringBufferSize must be a power of 2
     */
    public PersistenceBatchWriter(PostgresOrderRepository orderRepo,
                                  PostgresExecutionRepository executionRepo,
                                  PostgresLedgerRepository ledgerRepo,
                                  int batchSize,
                                  int ringBufferSize) {
        this.orderRepo = orderRepo;
        this.executionRepo = executionRepo;
        this.ledgerRepo = ledgerRepo;
        this.batchSize = batchSize;

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "persistence-writer-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };

        this.disruptor = new Disruptor<>(
                PersistenceEvent::new,
                ringBufferSize,
                threadFactory,
                ProducerType.MULTI,
                new SleepingWaitStrategy()
        );

        disruptor.handleEventsWith(new BatchingEventHandler());
        disruptor.setDefaultExceptionHandler(new PersistenceExceptionHandler());
        this.ringBuffer = disruptor.start();

        log.info("PersistenceBatchWriter started: batchSize={}, ringBufferSize={}", batchSize, ringBufferSize);
    }

    /**
     * Enqueues an order for async persistence.
     */
    public void enqueueOrder(OmsOrder order) {
        long sequence = ringBuffer.next();
        try {
            PersistenceEvent event = ringBuffer.get(sequence);
            event.type = EventType.ORDER;
            event.order = order;
            event.execution = null;
            event.ledgerEntries = null;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Enqueues an execution report for async persistence.
     */
    public void enqueueExecution(ExecutionReport execution) {
        long sequence = ringBuffer.next();
        try {
            PersistenceEvent event = ringBuffer.get(sequence);
            event.type = EventType.EXECUTION;
            event.order = null;
            event.execution = execution;
            event.ledgerEntries = null;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Enqueues a list of ledger entries for async batch persistence.
     */
    public void enqueueLedgerEntries(List<LedgerEntryRecord> entries) {
        long sequence = ringBuffer.next();
        try {
            PersistenceEvent event = ringBuffer.get(sequence);
            event.type = EventType.LEDGER;
            event.order = null;
            event.execution = null;
            event.ledgerEntries = entries;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    @Override
    public void close() {
        log.info("Shutting down PersistenceBatchWriter...");
        disruptor.shutdown();
        log.info("PersistenceBatchWriter shut down");
    }

    // ---- Event types ----

    enum EventType {
        ORDER, EXECUTION, LEDGER
    }

    static class PersistenceEvent {
        EventType type;
        OmsOrder order;
        ExecutionReport execution;
        List<LedgerEntryRecord> ledgerEntries;
    }

    // ---- Batching handler ----

    private class BatchingEventHandler implements EventHandler<PersistenceEvent> {

        private final List<OmsOrder> orderBatch = new ArrayList<>();
        private final List<ExecutionReport> executionBatch = new ArrayList<>();
        private final List<LedgerEntryRecord> ledgerBatch = new ArrayList<>();

        @Override
        public void onEvent(PersistenceEvent event, long sequence, boolean endOfBatch) {
            switch (event.type) {
                case ORDER -> orderBatch.add(event.order);
                case EXECUTION -> executionBatch.add(event.execution);
                case LEDGER -> ledgerBatch.addAll(event.ledgerEntries);
            }

            // Clear references to allow GC
            event.order = null;
            event.execution = null;
            event.ledgerEntries = null;

            int totalPending = orderBatch.size() + executionBatch.size() + ledgerBatch.size();

            if (endOfBatch || totalPending >= batchSize) {
                flush();
            }
        }

        private void flush() {
            flushOrders();
            flushExecutions();
            flushLedger();
        }

        private void flushOrders() {
            if (orderBatch.isEmpty()) {
                return;
            }
            try {
                for (OmsOrder order : orderBatch) {
                    orderRepo.saveOrder(order);
                }
            } catch (Exception e) {
                log.error("Failed to flush order batch size={}", orderBatch.size(), e);
            } finally {
                orderBatch.clear();
            }
        }

        private void flushExecutions() {
            if (executionBatch.isEmpty()) {
                return;
            }
            try {
                executionRepo.saveBatch(executionBatch);
            } catch (Exception e) {
                log.error("Failed to flush execution batch size={}", executionBatch.size(), e);
            } finally {
                executionBatch.clear();
            }
        }

        private void flushLedger() {
            if (ledgerBatch.isEmpty()) {
                return;
            }
            try {
                ledgerRepo.saveLedgerEntries(ledgerBatch);
            } catch (Exception e) {
                log.error("Failed to flush ledger batch size={}", ledgerBatch.size(), e);
            } finally {
                ledgerBatch.clear();
            }
        }
    }

    // ---- Exception handler ----

    private static class PersistenceExceptionHandler implements ExceptionHandler<PersistenceEvent> {

        @Override
        public void handleEventException(Throwable ex, long sequence, PersistenceEvent event) {
            log.error("Exception processing persistence event seq={} type={}",
                    sequence, event != null ? event.type : "null", ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            log.error("Exception on persistence handler start", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            log.error("Exception on persistence handler shutdown", ex);
        }
    }
}
