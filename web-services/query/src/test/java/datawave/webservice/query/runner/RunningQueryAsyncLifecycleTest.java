package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.util.concurrent.Uninterruptibles;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

/**
 * Exercises the real producer and request wait loops used by the asynchronous RunningQuery default introduced in PR #3637.
 *
 * Latches establish the ordering between prefetch, reset, and forced page returns. The Future.get deadlines are shorter than the ten-second page timeout, so
 * normal page expiration cannot make a blocked request appear to have responded to a force notification or freed an executor worker.
 */
@Timeout(15)
public class RunningQueryAsyncLifecycleTest {
    private final List<RunningQuery> queries = new ArrayList<>();
    private final List<ExecutorService> executors = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
    }

    @AfterEach
    public void tearDown() throws Exception {
        for (RunningQuery query : queries) {
            query.cancel();
        }
        for (ExecutorService executor : executors) {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS), "Query worker did not terminate");
        }
    }

    /**
     * After returning a page, the original producer could occupy a worker while blocked on its full read-ahead queue. With one shared worker, that idle query
     * prevented a second query's producer from starting. The worker must become available without losing the first query's buffered rows.
     */
    @Test
    public void idlePagedQueryDoesNotStarveAnotherQuery() throws Exception {
        ExecutorService executor = executor();
        CountDownLatch prefetched = new CountDownLatch(1);
        Iterator<Object> rows = Arrays.<Object> asList("a", "b", "c", "d", "e").iterator();
        AtomicInteger nextCalls = new AtomicInteger();
        Iterator<Object> iterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public Object next() {
                Object row = rows.next();
                if (nextCalls.incrementAndGet() == 3) {
                    // Reading the third row establishes that a later row was enqueued before submitting the competing request.
                    prefetched.countDown();
                }
                return row;
            }
        };
        RunningQuery first = query(executor, iterator, 1);
        RunningQuery second = query(executor, Collections.<Object> singletonList("fast-result").iterator(), 1);

        assertEquals(Collections.singletonList("a"), first.next().getResults());
        assertTrue(prefetched.await(2, TimeUnit.SECONDS), "First query did not prefetch");
        // Run the request on a separate executor: only its producer should compete for the shared query worker.
        Future<ResultsPage> page = executor().submit(second::next);
        assertEquals(Collections.singletonList("fast-result"), page.get(2, TimeUnit.SECONDS).getResults());

        // Returning the worker must preserve prefetched rows for later requests.
        List<Object> remaining = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            remaining.addAll(first.next().getResults());
        }
        assertEquals(Arrays.asList("b", "c", "d", "e"), remaining);
        assertEquals(ResultsPage.Status.NONE, first.next().getStatus());
    }

    /**
     * Terminating a producer during close did not mark the execution stopped. An empty-buffer request woke up and submitted another producer on the same
     * iterator, potentially while the old iterator call was still alive and its resources were being closed. Closing either wait must finish the request
     * without restarting production; only reinitialization may enable a replacement producer.
     *
     * @param blockInNext
     *            whether to hold the producer in next() rather than hasNext()
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void closeDuringWaitStopsProductionUntilReset(boolean blockInNext) throws Exception {
        AtomicInteger submittedTasks = new AtomicInteger();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                submittedTasks.incrementAndGet();
                super.execute(task);
            }
        };
        executors.add(executor);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger hasNextCalls = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (hasNextCalls.incrementAndGet() > 1) {
                    // A resubmitted producer can finish promptly, so the assertion identifies the restart rather than a timeout.
                    return false;
                }
                if (!blockInNext) {
                    awaitRelease();
                }
                return true;
            }

            @Override
            public Object next() {
                if (blockInNext) {
                    awaitRelease();
                }
                return "old-row";
            }

            private void awaitRelease() {
                blocked.countDown();
                // Keep the old invocation alive despite Future.cancel(true), exposing concurrent access by a resubmitted producer.
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS), "Closed producer was not released");
            }
        };
        RunningQuery query = query(executor, rows, 10);
        Future<ResultsPage> page = executor().submit(query::next);
        try {
            assertTrue(blocked.await(2, TimeUnit.SECONDS), "Producer did not block");
            awaitResultWait(query, blockInNext ? "getNext" : "hasNext");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            assertEquals(ResultsPage.Status.NONE, page.get(1, TimeUnit.SECONDS).getStatus());
            // Count submissions as well as iterator calls: a restarted task can be canceled before it reaches hasNext().
            assertEquals(1, submittedTasks.get(), "Close submitted another producer on the old execution");
            assertEquals(1, hasNextCalls.get(), "Close resubmitted production on the old iterator");

            // A reset must still start normally on the other worker while the canceled old call remains alive.
            when(query.getLogic().getTransformIterator(query.getSettings()))
                            .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new-row").iterator(), value -> value));
            query.setClient(mock(AccumuloClient.class));
            assertEquals(Collections.singletonList("new-row"), query.next().getResults());
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            release.countDown();
        }
    }

    /**
     * The generation guard in hasNext() stops a request already waiting there, but its generation is no longer available after that method returns. Close after
     * a pending row is advertised and before getNext() begins: production must remain stopped across this boundary instead of submitting a new task on the
     * iterator whose original next() call is still alive.
     */
    @Test
    public void closeBetweenHasNextAndGetNextDoesNotRestartProduction() throws Exception {
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        CountDownLatch callerPaused = new CountDownLatch(1);
        CountDownLatch releaseCaller = new CountDownLatch(1);
        AtomicInteger hasNextCalls = new AtomicInteger();
        AtomicInteger nextCalls = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return hasNextCalls.incrementAndGet() <= 2;
            }

            @Override
            public Object next() {
                if (nextCalls.incrementAndGet() == 1) {
                    return "first";
                }
                producerBlocked.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseProducer, 5, TimeUnit.SECONDS), "Old producer was not released");
                return "pending";
            }
        };
        RunningQuery query = query(executor(2), rows, 10);
        // The timing heuristic runs after hasNext() advertised the pending row and before getNext() tries to consume it.
        doAnswer(invocation -> {
            callerPaused.countDown();
            assertTrue(releaseCaller.await(2, TimeUnit.SECONDS), "Caller was not released");
            return false;
        }).when(query.getTiming()).shouldReturnPartialResults(anyInt(), anyInt(), anyLong());
        Future<ResultsPage> page = executor().submit(query::next);
        try {
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Producer did not block");
            assertTrue(callerPaused.await(2, TimeUnit.SECONDS), "Caller did not pause before getNext");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            releaseCaller.countDown();
            assertEquals(Collections.singletonList("first"), page.get(2, TimeUnit.SECONDS).getResults());
            assertEquals(2, hasNextCalls.get(), "Close restarted the producer between hasNext and getNext");
        } finally {
            releaseCaller.countDown();
            releaseProducer.countDown();
        }
    }

    /**
     * Successful initialization normally re-enables production. If close terminates that execution while initialize() is still running, its later completion
     * must not undo the close. Hold initialization across close, then verify that only a subsequent initialization can start the replacement iterator.
     */
    @Test
    public void closeDuringInitializationDoesNotReopenProduction() throws Exception {
        RunningQuery query = query(executor(), Collections.<Object> singletonList("new-row").iterator(), 1);
        CountDownLatch initializing = new CountDownLatch(1);
        CountDownLatch releaseInitializer = new CountDownLatch(1);
        GenericQueryConfiguration configuration = mock(GenericQueryConfiguration.class);
        when(query.getLogic().initialize(any(), any(), any())).thenAnswer(invocation -> {
            initializing.countDown();
            assertTrue(releaseInitializer.await(2, TimeUnit.SECONDS), "Initializer was not released");
            return configuration;
        });
        Future<?> initialization = executor().submit(() -> {
            query.setClient(mock(AccumuloClient.class));
            return null;
        });
        try {
            assertTrue(initializing.await(2, TimeUnit.SECONDS), "Initialization did not start");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            releaseInitializer.countDown();
            initialization.get(2, TimeUnit.SECONDS);
            assertNull(query.getClient());
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus(), "The closed initializer reopened production");

            // A new attempt after close is authorized to re-enable production.
            query.setClient(mock(AccumuloClient.class));
            assertEquals(Collections.singletonList("new-row"), query.next().getResults());
        } finally {
            releaseInitializer.countDown();
        }
    }

    /**
     * Reset cleared returned-result usage but retained the next/seek counters used by maxWork. Checking that budget before starting the replacement producer
     * returned NONE immediately, so its fresh transformer could never publish fresh counters. Keep maxWork constant across both executions and reach it during
     * old prefetch only after the first page has returned.
     *
     * @param seekBudget
     *            whether seekCount rather than nextCount reaches the old execution's work limit
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void resetRestartsNextAndSeekWorkBudgets(boolean seekBudget) throws Exception {
        ExecutorService executor = executor();
        CountDownLatch releaseSecondRow = new CountDownLatch(1);
        AtomicInteger nextCalls = new AtomicInteger();
        MetricsTransformer oldTransformer = new MetricsTransformer(seekBudget ? 0 : 1, seekBudget ? 1 : 0) {
            @Override
            public Object transform(Object value) {
                if (nextCalls.incrementAndGet() == 2) {
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseSecondRow, 5, TimeUnit.SECONDS), "Old prefetch was not released");
                    setWorkCounts(seekBudget ? 0 : 100, seekBudget ? 100 : 0);
                }
                return value;
            }
        };
        RunningQuery query = query(executor, Collections.<Object> singletonList("unused").iterator(), 1);
        when(query.getLogic().getMaxWork()).thenReturn(100L);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Arrays.<Object> asList("old-row", "old-prefetch").iterator(), oldTransformer));
        query.setClient(mock(AccumuloClient.class));
        try {
            assertEquals(Collections.singletonList("old-row"), query.next().getResults());
            releaseSecondRow.countDown();
            // This marker proves every old metric write has completed before close/reset; this case isolates retained budget.
            executor.submit(() -> {}).get(2, TimeUnit.SECONDS);
            assertWorkMetrics(query, seekBudget ? 0 : 100, seekBudget ? 100 : 0);
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            when(query.getLogic().getTransformIterator(query.getSettings()))
                            .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new-row").iterator(), new MetricsTransformer(1, 1)));
            query.setClient(mock(AccumuloClient.class));

            assertWorkMetrics(query, 0, 0);
            assertEquals(Collections.singletonList("new-row"), query.next().getResults());
            executor.submit(() -> {}).get(2, TimeUnit.SECONDS);
            assertWorkMetrics(query, 1, 1);
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releaseSecondRow.countDown();
        }
    }

    /**
     * The generation check preceded the metric callback, leaving a gap: a canceled writer already inside that callback could overwrite the live work counters
     * after reset. With two producer workers, keep that old callback alive while the new execution produces and publishes metrics. Its late completion must
     * neither change any work counters nor make maxWork truncate the remaining new rows.
     */
    @Test
    public void resetIgnoresLateMetricPublicationFromPreviousProducer() throws Exception {
        AtomicReference<Runnable> oldTask = new AtomicReference<>();
        CountDownLatch oldProducerCompleted = new CountDownLatch(1);
        CountDownLatch newProducerCompleted = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                oldTask.compareAndSet(null, task);
                super.execute(task);
            }

            @Override
            protected void afterExecute(Runnable task, Throwable failure) {
                // Observe actual task completion, not Future completion: cancel(true) can finish the Future while its callback still runs.
                if (task == oldTask.get()) {
                    oldProducerCompleted.countDown();
                } else {
                    newProducerCompleted.countDown();
                }
                super.afterExecute(task, failure);
            }
        };
        executors.add(executor);
        CountDownLatch oldWriterEntered = new CountDownLatch(1);
        CountDownLatch releaseOldWriter = new CountDownLatch(1);
        MetricsTransformer oldTransformer = new MetricsTransformer(100, 100) {
            @Override
            public void writeQueryMetrics(BaseQueryMetric metric) {
                oldWriterEntered.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseOldWriter, 5, TimeUnit.SECONDS), "Old metric writer was not released");
                super.writeQueryMetrics(metric);
            }
        };
        RunningQuery query = query(executor, Collections.<Object> singletonList("unused").iterator(), 1);
        when(query.getLogic().getMaxWork()).thenReturn(50L);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("old-row").iterator(), oldTransformer));
        query.setClient(mock(AccumuloClient.class));
        query.setActiveCall(true);
        Future<ResultsPage> oldPage = executor().submit(query::next);
        try {
            assertTrue(oldWriterEntered.await(2, TimeUnit.SECONDS), "Old metric writer did not enter");
            // The old implementation looks ahead even when this one-row page is full. Force that request to return so the callback
            // stays blocked across reset; letting its latch expire would hide the late-publication race.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!oldPage.isDone() && query.getCurrentPageCount() == 0 && System.nanoTime() < deadline) {
                Thread.yield();
            }
            if (!oldPage.isDone()) {
                assertEquals(1, query.getCurrentPageCount(), "Old request did not collect its row");
                query.attemptForcedPageReturn();
            }
            assertEquals(Collections.singletonList("old-row"), oldPage.get(2, TimeUnit.SECONDS).getResults());
            assertEquals(1L, oldProducerCompleted.getCount(), "Old callback must remain alive across reset");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            when(query.getLogic().getTransformIterator(query.getSettings()))
                            .thenReturn(new TransformIterator<>(Arrays.<Object> asList("new-a", "new-b", "new-c").iterator(), new MetricsTransformer(1, 1)));
            query.setClient(mock(AccumuloClient.class));
            assertEquals(Collections.singletonList("new-a"), query.next().getResults());
            assertTrue(newProducerCompleted.await(2, TimeUnit.SECONDS), "Replacement producer did not finish prefetch");
            assertWorkMetrics(query, 1, 1);

            // The replacement has already published its work counters. Let the canceled callback finish only now.
            releaseOldWriter.countDown();
            assertTrue(oldProducerCompleted.await(2, TimeUnit.SECONDS), "Old producer did not finish its metric callback");
            assertWorkMetrics(query, 1, 1);
            assertEquals(Collections.singletonList("new-b"), query.next().getResults());
            assertEquals(Collections.singletonList("new-c"), query.next().getResults());
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releaseOldWriter.countDown();
        }
    }

    /**
     * A producer can mark itself stopped before its executor worker is available for another task. Hold the worker in afterExecute() to reproduce this handoff
     * window with a bounded executor. Refilling after dequeuing a buffered row previously threw RejectedExecutionException and discarded the page. Buffered
     * pages must still be delivered; a larger page must wait for capacity and resume without losing or duplicating rows.
     *
     * @param needsRefill
     *            whether the second page needs another producer invocation after consuming the buffered rows
     * @param forceWhileRejected
     *            whether to force a partial page while waiting for executor capacity
     * @param expireWhileRejected
     *            whether to expire the page deadline while waiting for executor capacity
     */
    @ParameterizedTest
    @CsvSource({"false, false, false", "true, false, false", "true, true, false", "true, false, true"})
    public void executorHandoffPreservesBufferedRows(boolean needsRefill, boolean forceWhileRejected, boolean expireWhileRejected) throws Exception {
        CountDownLatch workerReturning = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        AtomicBoolean holdFirstTask = new AtomicBoolean(true);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new SynchronousQueue<>()) {
            @Override
            protected void afterExecute(Runnable task, Throwable failure) {
                if (holdFirstTask.compareAndSet(true, false)) {
                    workerReturning.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseWorker, 5, TimeUnit.SECONDS), "Executor worker was not released");
                }
                super.afterExecute(task, failure);
            }
        };
        executors.add(executor);
        CountDownLatch releasePrefetch = new CountDownLatch(1);
        CountDownLatch prefetched = new CountDownLatch(1);
        AtomicInteger nextCalls = new AtomicInteger();
        Iterator<Object> rows = new TransformIterator<>(Arrays.<Object> asList("a", "b", "c", "d").iterator(), value -> {
            int count = nextCalls.incrementAndGet();
            if (count == 2) {
                // Keep the producer alive until the request has safely dequeued its first row.
                assertTrue(Uninterruptibles.awaitUninterruptibly(releasePrefetch, 5, TimeUnit.SECONDS), "Prefetch was not released");
            } else if (count == 3) {
                prefetched.countDown();
            }
            return value;
        });
        RunningQuery query = query(executor, rows, 1);
        CountDownLatch callerPaused = new CountDownLatch(1);
        CountDownLatch releaseCaller = new CountDownLatch(1);
        AtomicBoolean pauseFirstPage = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (query.getCurrentPageCount() == 1 && pauseFirstPage.compareAndSet(true, false)) {
                callerPaused.countDown();
                assertTrue(releaseCaller.await(2, TimeUnit.SECONDS), "First page was not released");
            }
            return invocation.callRealMethod();
        }).when(query.getSettings()).getUncaughtExceptionHandler();
        ExecutorService requests = executor();
        Future<ResultsPage> firstPage = requests.submit(query::next);
        try {
            assertTrue(callerPaused.await(2, TimeUnit.SECONDS), "First page did not collect a row");
            releasePrefetch.countDown();
            assertTrue(prefetched.await(2, TimeUnit.SECONDS), "Producer did not read ahead");
            releaseCaller.countDown();
            assertEquals(Collections.singletonList("a"), firstPage.get(2, TimeUnit.SECONDS).getResults());
            // The producer has filled the queue and returned, but the executor still rejects submissions.
            assertTrue(workerReturning.await(2, TimeUnit.SECONDS), "Idle producer did not yield");
            query.getSettings().setPagesize(needsRefill ? 3 : 1);
            query.setActiveCall(true);
            if (expireWhileRejected) {
                // Keep the usual ten-second timeout for setup; only this refill uses a short deadline.
                when(query.getTiming().getPageShortCircuitTimeoutMs()).thenReturn(250L);
            }
            Future<ResultsPage> secondPage = requests.submit(query::next);
            if (needsRefill) {
                awaitResultWait(query, "hasNext");
                if (forceWhileRejected || expireWhileRejected) {
                    if (forceWhileRejected) {
                        // Retrying executor handoff must obey the same force flag as waiting for iterator work.
                        query.attemptForcedPageReturn();
                    }
                    // Both force and expiration must preserve the partial page and the unproduced final row.
                    ResultsPage partialPage = secondPage.get(2, TimeUnit.SECONDS);
                    assertEquals(Arrays.asList("b", "c"), partialPage.getResults());
                    assertEquals(ResultsPage.Status.PARTIAL, partialPage.getStatus());
                    assertFalse(query.isFinished());
                    releaseWorker.countDown();
                    assertEquals(Collections.singletonList("d"), query.next().getResults());
                } else {
                    releaseWorker.countDown();
                    assertEquals(Arrays.asList("b", "c", "d"), secondPage.get(2, TimeUnit.SECONDS).getResults());
                }
            } else {
                assertEquals(Collections.singletonList("b"), secondPage.get(1, TimeUnit.SECONDS).getResults());
                // Even the final buffered row must be delivered before executor capacity returns.
                assertEquals(Collections.singletonList("c"), requests.submit(query::next).get(1, TimeUnit.SECONDS).getResults());
                releaseWorker.countDown();
                assertEquals(Collections.singletonList("d"), query.next().getResults());
            }
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releaseCaller.countDown();
            releasePrefetch.countDown();
            releaseWorker.countDown();
        }
    }

    /**
     * Yielding on every full two-row queue also yielded during active page consumption, approaching one executor submission per result. Pace the consumer so
     * the producer fills its queue repeatedly, then assert a bounded task count for one large page. This checks scheduling behavior without asserting elapsed
     * throughput or requiring a particular executor/consumer race.
     */
    @Test
    public void activePageDoesNotResubmitProducerForEveryRow() throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        executors.add(executor);
        List<Object> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(i);
        }
        RunningQuery query = query(executor, rows.iterator(), rows.size());
        when(query.getLogic().getMaxPageSize()).thenReturn(rows.size());
        query.getSettings().setMaxResultsOverridden(true);
        query.getSettings().setMaxResultsOverride((long) rows.size());
        doAnswer(invocation -> {
            if (query.getCurrentPageCount() > 0) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            }
            return invocation.callRealMethod();
        }).when(query.getSettings()).getUncaughtExceptionHandler();

        assertEquals(rows, query.next().getResults());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        assertTrue(executor.getTaskCount() <= 2, "One page submitted " + executor.getTaskCount() + " producer tasks");
    }

    /**
     * An unavailable executor must not be mistaken for iterator exhaustion. Deferring a temporary handoff rejection must still propagate rejection when the
     * executor has actually shut down and no results can be produced.
     */
    @Test
    public void shutdownExecutorStillRejectsAnUnstartedQuery() throws Exception {
        ExecutorService executor = executor();
        RunningQuery query = query(executor, Collections.<Object> singletonList("a").iterator(), 1);
        executor.shutdown();
        assertThrows(RejectedExecutionException.class, query::next);
    }

    /**
     * Clearing the producer's local failure did not clear the real query exception handler, which retains its first error. Reset therefore failed during
     * setClient() before the healthy replacement iterator could run. A fresh execution must have its own handler, ignore late reports to the old handler, and
     * continue to report failures from the replacement execution.
     */
    @Test
    public void resetAfterRecordedProducerFailureUsesFreshExceptionHandler() throws Exception {
        ExecutorService executor = executor();
        IllegalStateException oldFailure = new IllegalStateException("Old producer failure");
        RunningQuery query = query(executor, new TransformIterator<>(Collections.<Object> singletonList("old").iterator(), value -> {
            throw oldFailure;
        }), 1);
        QueryUncaughtExceptionHandler oldHandler = new QueryUncaughtExceptionHandler();
        query.getSettings().setUncaughtExceptionHandler(oldHandler);
        assertThrows(Exception.class, query::next);
        assertSame(oldFailure, oldHandler.getUncaughtException().getLeft());
        query.closeConnection(mock(AccumuloConnectionFactory.class));
        executor.submit(() -> {}).get(2, TimeUnit.SECONDS);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new").iterator(), value -> value));
        query.setClient(mock(AccumuloClient.class));

        QueryUncaughtExceptionHandler newHandler = query.getSettings().getUncaughtExceptionHandler();
        assertNotSame(oldHandler, newHandler);
        assertFalse(newHandler.hasUncaughtException());
        oldHandler.uncaughtException(Thread.currentThread(), new IllegalStateException("Late old scanner failure"));
        assertEquals(Collections.singletonList("new"), query.next().getResults());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());

        query.closeConnection(mock(AccumuloConnectionFactory.class));
        IllegalStateException newFailure = new IllegalStateException("New producer failure");
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("broken").iterator(), value -> {
                            throw newFailure;
                        }));
        query.setClient(mock(AccumuloClient.class));
        assertNotSame(newHandler, query.getSettings().getUncaughtExceptionHandler(), "Even a healthy execution needs a fresh handler on reset");
        newHandler.uncaughtException(Thread.currentThread(), new IllegalStateException("Late failure from the healthy execution"));
        assertThrows(Exception.class, query::next);
        assertSame(newFailure, query.getSettings().getUncaughtExceptionHandler().getUncaughtException().getLeft());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void yieldedProducerResumesInsteadOfReportingEndOfResults(boolean beforeWait) throws Exception {
        RunningQuery query = query(executor(), Arrays.<Object> asList("unread-a", "unread-b", "unread-c").iterator(), 10);
        AtomicBoolean running = (AtomicBoolean) ReflectionTestUtils.getField(query, "running");
        AtomicInteger hasNext = (AtomicInteger) ReflectionTestUtils.getField(query, "hasNext");
        // Reconstruct an empty queue after the producer decides to yield, but before it clears running.
        // Its exit must resume production whether it precedes or follows the request's wait.
        running.set(true);
        Future<ResultsPage> page;
        if (beforeWait) {
            synchronized (hasNext) {
                page = executor().submit(query::next);
                awaitResultWait(query, "hasNext", Thread.State.BLOCKED);
                synchronized (running) {
                    running.set(false);
                }
            }
        } else {
            page = executor().submit(query::next);
            awaitResultWait(query, "hasNext");
            synchronized (running) {
                running.set(false);
            }
            synchronized (hasNext) {
                hasNext.notifyAll();
            }
        }

        ResultsPage results = page.get(2, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("unread-a", "unread-b", "unread-c"), results.getResults());
        assertEquals(ResultsPage.Status.COMPLETE, results.getStatus());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    @Test
    public void terminatedProducerDoesNotRestartWaitingRequest() throws Exception {
        RunningQuery query = query(executor(), Collections.<Object> singletonList("unread").iterator(), 10);
        AtomicBoolean running = (AtomicBoolean) ReflectionTestUtils.getField(query, "running");
        running.set(true);
        Future<ResultsPage> page = executor().submit(query::next);
        awaitResultWait(query, "hasNext");

        query.terminateResultsThread();

        assertEquals(ResultsPage.Status.NONE, page.get(2, TimeUnit.SECONDS).getStatus());
        assertFalse(running.get(), "Explicit termination must not schedule another producer");
    }

    /**
     * Reset reused the RunningQuery and reset its page number, but retained prefetched rows, iterator counters, and the number of results already returned. A
     * fresh execution must start with new-a, page one, and enough result budget to return both new rows.
     */
    @Test
    public void resetDiscardsPrefetchedRowsAndRestartsResultBudget() throws Exception {
        ExecutorService executor = executor();
        CountDownLatch prefetched = new CountDownLatch(1);
        AtomicInteger nextCalls = new AtomicInteger();
        Iterator<Object> rows = new TransformIterator<>(Arrays.<Object> asList("old-a", "old-b", "old-c", "old-d").iterator(), value -> {
            if (nextCalls.incrementAndGet() == 3) {
                // Wait until old-b has been enqueued; otherwise canceling before prefetch could hide the retained-row bug.
                prefetched.countDown();
            }
            return value;
        });
        RunningQuery query = query(executor, rows, 1);
        query.getSettings().setMaxResultsOverridden(true);
        query.getSettings().setMaxResultsOverride(2L);

        assertEquals(Collections.singletonList("old-a"), query.next().getResults());
        assertTrue(prefetched.await(2, TimeUnit.SECONDS), "Old execution did not prefetch");
        query.closeConnection(mock(AccumuloConnectionFactory.class));
        // A marker on the same single-worker executor proves the old producer exited before reinitialization.
        // This case isolates retained state; late producer completion is covered separately below.
        executor.submit(() -> {}).get(2, TimeUnit.SECONDS);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Arrays.<Object> asList("new-a", "new-b").iterator(), value -> value));
        query.setClient(mock(AccumuloClient.class));

        assertEquals(Collections.singletonList("new-a"), query.next().getResults());
        assertEquals(1, query.getLastPageNumber());
        assertEquals(Collections.singletonList("new-b"), query.next().getResults());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    /**
     * Canceling a Future does not prove its iterator call has stopped. Keep the old next() call alive across reset, then let it return a row or throw. Neither
     * its output nor its completion may affect the newly installed iterator's results or producer state.
     *
     * @param lateFailure
     *            whether the old iterator throws instead of returning its pending row
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void resetIgnoresLateResultsAndFailuresFromPreviousProducer(boolean lateFailure) throws Exception {
        ExecutorService executor = executor();
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return count.get() < 2;
            }

            @Override
            public Object next() {
                if (count.getAndIncrement() == 0) {
                    return "old-a";
                }
                producerBlocked.countDown();
                // Model iterator work that does not stop immediately when closeConnection interrupts its Future.
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseProducer, 5, TimeUnit.SECONDS), "Old producer was not released");
                if (lateFailure) {
                    throw new IllegalStateException("Failure from the previous execution");
                }
                return "old-b";
            }
        };
        RunningQuery query = query(executor, rows, 1);
        try {
            assertEquals(Collections.singletonList("old-a"), query.next().getResults());
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Old producer did not block");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            when(query.getLogic().getTransformIterator(query.getSettings()))
                            .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new-a").iterator(), value -> value));
            query.setClient(mock(AccumuloClient.class));
            // Release the old invocation only after the replacement execution has been initialized.
            releaseProducer.countDown();
            assertEquals(Collections.singletonList("new-a"), query.next().getResults());
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releaseProducer.countDown();
        }
    }

    /**
     * A force notification could be sent after collecting a row but before the request entered hasNext()'s monitor wait. The old wait predicate ignored
     * forcedReturn, so the request started waiting after the notification and could not reach the flag check inside the results loop.
     */
    @Test
    public void forcedReturnBeforeHasNextWaitReturnsCollectedRows() throws Exception {
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        RunningQuery query = query(executor(), blockingIterator(false, producerBlocked, releaseProducer), 10);
        query.setActiveCall(true);
        CountDownLatch callerPaused = new CountDownLatch(1);
        CountDownLatch releaseCaller = new CountDownLatch(1);
        AtomicBoolean paused = new AtomicBoolean();
        QueryImpl settings = (QueryImpl) query.getSettings();
        // RunningQuery checks the handler after incrementing its page count and before the next loop iteration.
        // Pause at that existing call to place the force notification before the request starts waiting.
        doAnswer(invocation -> {
            if (query.getCurrentPageCount() == 1 && paused.compareAndSet(false, true)) {
                callerPaused.countDown();
                assertTrue(releaseCaller.await(2, TimeUnit.SECONDS), "Caller was not released");
            }
            return null;
        }).when(settings).getUncaughtExceptionHandler();
        Future<ResultsPage> page = executor().submit(query::next);
        try {
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Producer did not block");
            assertTrue(callerPaused.await(2, TimeUnit.SECONDS), "Caller did not pause after its first row");
            // No request monitor is waiting yet: the flag, not a remembered notification, must prevent the subsequent wait.
            query.attemptForcedPageReturn();
            releaseCaller.countDown();
            ResultsPage results = page.get(1, TimeUnit.SECONDS);
            assertEquals(Collections.singletonList("first"), results.getResults());
            assertEquals(ResultsPage.Status.PARTIAL, results.getStatus());
            assertFalse(query.isFinished(), "A forced return must preserve the pending result");
        } finally {
            releaseCaller.countDown();
            releaseProducer.countDown();
        }
    }

    /**
     * Covers the already-waiting counterpart to the missed-notification case. Forcing either wait must return a partial page with the collected row, without
     * treating the pending second row as end-of-query or consuming its iterator counters.
     *
     * @param blockInNext
     *            false blocks the request in hasNext(); true advertises another row but blocks its production, leaving the request in getNext()
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void forcedReturnDuringWaitPreservesPendingRow(boolean blockInNext) throws Exception {
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        RunningQuery query = query(executor(), blockingIterator(blockInNext, producerBlocked, releaseProducer), 10);
        query.setActiveCall(true);
        Future<ResultsPage> page = executor().submit(query::next);
        try {
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Producer did not block");
            awaitResultWait(query, blockInNext ? "getNext" : "hasNext");
            query.attemptForcedPageReturn();
            ResultsPage results = page.get(1, TimeUnit.SECONDS);
            assertEquals(Collections.singletonList("first"), results.getResults());
            assertEquals(ResultsPage.Status.PARTIAL, results.getStatus());
            assertFalse(query.isFinished());
            releaseProducer.countDown();
            assertEquals(Collections.singletonList("second"), query.next().getResults());
        } finally {
            releaseProducer.countDown();
        }
    }

    private void awaitResultWait(RunningQuery query, String method) {
        awaitResultWait(query, method, Thread.State.TIMED_WAITING);
    }

    private void awaitResultWait(RunningQuery query, String method, Thread.State state) {
        // The producer latch only establishes that the iterator is blocked. Check the request's actual monitor wait
        // before forcing, so these cases cannot accidentally exercise the before-wait scenario instead.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            Thread thread = query.getCurrentThread();
            if (thread != null && thread.getState() == state) {
                for (StackTraceElement frame : thread.getStackTrace()) {
                    if (frame.getClassName().equals(RunningQuery.class.getName()) && frame.getMethodName().equals(method)) {
                        return;
                    }
                }
            }
            Thread.yield();
        }
        throw new AssertionError("Request did not reach " + state + " in " + method);
    }

    private Iterator<Object> blockingIterator(boolean blockInNext, CountDownLatch blocked, CountDownLatch release) {
        return new Iterator<>() {
            private int count;

            @Override
            public boolean hasNext() {
                if (count == 1 && !blockInNext) {
                    awaitRelease();
                }
                return count < 2;
            }

            @Override
            public Object next() {
                if (count == 1 && blockInNext) {
                    awaitRelease();
                }
                return count++ == 0 ? "first" : "second";
            }

            private void awaitRelease() {
                blocked.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS), "Producer was not released");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    private ExecutorService executor() {
        return executor(1);
    }

    private ExecutorService executor(int workers) {
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        executors.add(executor);
        return executor;
    }

    private void assertWorkMetrics(RunningQuery query, long nextCount, long seekCount) {
        BaseQueryMetric metric = query.getMetric();
        long total = nextCount + seekCount;
        assertEquals(total, metric.getSourceCount(), "Source count from a different execution");
        assertEquals(nextCount, metric.getNextCount(), "Next count from a different execution");
        assertEquals(seekCount, metric.getSeekCount(), "Seek count from a different execution");
        assertEquals(total, metric.getYieldCount(), "Yield count from a different execution");
        assertEquals(total, metric.getDocRanges(), "Document ranges from a different execution");
        assertEquals(total, metric.getFiRanges(), "Field-index ranges from a different execution");
    }

    private static class MetricsTransformer implements Transformer<Object,Object>, WritesQueryMetrics {
        private long nextCount;
        private long seekCount;

        private MetricsTransformer(long nextCount, long seekCount) {
            setWorkCounts(nextCount, seekCount);
        }

        protected void setWorkCounts(long nextCount, long seekCount) {
            this.nextCount = nextCount;
            this.seekCount = seekCount;
        }

        @Override
        public Object transform(Object value) {
            return value;
        }

        @Override
        public void writeQueryMetrics(BaseQueryMetric metric) {
            metric.setSourceCount(getSourceCount());
            metric.setNextCount(getNextCount());
            metric.setSeekCount(getSeekCount());
            metric.setYieldCount(getYieldCount());
            metric.setDocRanges(getDocRanges());
            metric.setFiRanges(getFiRanges());
        }

        @Override
        public boolean hasMetrics() {
            return true;
        }

        @Override
        public long getSourceCount() {
            return nextCount + seekCount;
        }

        @Override
        public long getNextCount() {
            return nextCount;
        }

        @Override
        public long getSeekCount() {
            return seekCount;
        }

        @Override
        public long getYieldCount() {
            return nextCount + seekCount;
        }

        @Override
        public long getDocRanges() {
            return nextCount + seekCount;
        }

        @Override
        public long getFiRanges() {
            return nextCount + seekCount;
        }

        @Override
        public void resetMetrics() {
            setWorkCounts(0, 0);
        }
    }

    @SuppressWarnings("unchecked")
    private RunningQuery query(ExecutorService executor, Iterator<Object> rows, int pageSize) throws Exception {
        QueryImpl settings = spy(new QueryImpl());
        settings.setId(UUID.randomUUID());
        settings.setQueryLogicName("test");
        settings.setQuery("FOO == 'bar'");
        settings.setQueryName("async lifecycle test");
        settings.setUserDN("CN=user, OU=People");
        settings.setQueryAuthorizations("");
        settings.setBeginDate(new Date());
        settings.setEndDate(new Date());
        settings.setExpirationDate(new Date(System.currentTimeMillis() + 60000));
        settings.setPagesize(pageSize);
        QueryLogic<Object> logic = mock(QueryLogic.class);
        when(logic.getMaxPageSize()).thenReturn(100);
        when(logic.getMaxWork()).thenReturn(-1L);
        when(logic.getResultLimit(settings)).thenReturn(100L);
        when(logic.getMaxResults()).thenReturn(100L);
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getTransformIterator(settings)).thenReturn(new TransformIterator<>(rows, value -> value));
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("CN=user, OU=People", "CN=issuer"), DatawaveUser.UserType.USER, null, null, null, 0L);
        RunningQuery query = new RunningQuery(mock(AccumuloClient.class), AccumuloConnectionFactory.Priority.NORMAL, logic, settings, "",
                        new DatawavePrincipal(Collections.singletonList(user)), spy(new RunningQuery.RunningQueryTiming() {
                            @Override
                            public boolean shouldReturnPartialResults(int size, int maximum, long elapsed) {
                                // Disable the ordinary partial-page heuristic so only the explicit force can return these small pages promptly.
                                return false;
                            }

                            @Override
                            public int getMaxLongRunningTimeoutRetries() {
                                return 10;
                            }

                            @Override
                            public long getPageShortCircuitTimeoutMs() {
                                return 10000;
                            }
                        }), new QueryMetricFactoryImpl());
        query.setExecutor(executor);
        queries.add(query);
        return query;
    }
}
