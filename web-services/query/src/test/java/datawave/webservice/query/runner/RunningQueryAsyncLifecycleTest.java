package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.metric.QueryMetricsBean;
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
        AtomicBoolean resourcesOpen = new AtomicBoolean();
        GenericQueryConfiguration configuration = mock(GenericQueryConfiguration.class);
        doAnswer(invocation -> {
            resourcesOpen.set(false);
            return null;
        }).when(query.getLogic()).close();
        when(query.getLogic().initialize(any(), any(), any())).thenAnswer(invocation -> {
            initializing.countDown();
            assertTrue(releaseInitializer.await(2, TimeUnit.SECONDS), "Initializer was not released");
            // Model planning resources acquired after close has already cleaned up the previous ones.
            resourcesOpen.set(true);
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
            assertFalse(resourcesOpen.get(), "Resources acquired by the closed initializer were not released");
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus(), "The closed initializer reopened production");

            // A new attempt after close is authorized to re-enable production.
            query.setClient(mock(AccumuloClient.class));
            assertEquals(Collections.singletonList("new-row"), query.next().getResults());
        } finally {
            releaseInitializer.countDown();
        }
    }

    /**
     * The stale-initializer cleanup handled normal returns only. An initializer/setup that acquired resources after close and then threw bypassed that cleanup,
     * leaving its late resources open. Hold either phase across close, then fail after opening a resource; the original failure must still reach the caller
     * while the late resource is reclaimed.
     *
     * @param failInSetup
     *            whether setupQuery() rather than initialize() acquires the late resource and throws
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void initializationFailureAfterCloseReclaimsLateResources(boolean failInSetup) throws Exception {
        RunningQuery query = query(executor(), Collections.<Object> singletonList("unused").iterator(), 1);
        CountDownLatch initializing = new CountDownLatch(1);
        CountDownLatch releaseInitializer = new CountDownLatch(1);
        AtomicBoolean resourcesOpen = new AtomicBoolean();
        IllegalStateException failure = new IllegalStateException("Initialization failed after close");
        doAnswer(invocation -> {
            resourcesOpen.set(false);
            return null;
        }).when(query.getLogic()).close();
        if (failInSetup) {
            doAnswer(invocation -> {
                initializing.countDown();
                assertTrue(releaseInitializer.await(2, TimeUnit.SECONDS), "Setup was not released");
                resourcesOpen.set(true);
                throw failure;
            }).when(query.getLogic()).setupQuery(any());
        } else {
            when(query.getLogic().initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(releaseInitializer.await(2, TimeUnit.SECONDS), "Initializer was not released");
                resourcesOpen.set(true);
                throw failure;
            });
        }
        Future<?> initialization = executor().submit(() -> {
            query.setClient(mock(AccumuloClient.class));
            return null;
        });
        try {
            assertTrue(initializing.await(2, TimeUnit.SECONDS), "Initialization did not reach its gate");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            releaseInitializer.countDown();
            ExecutionException thrown = assertThrows(ExecutionException.class, () -> initialization.get(2, TimeUnit.SECONDS));
            assertSame(failure, thrown.getCause(), "Cleanup must not replace the original initialization failure");
            assertFalse(resourcesOpen.get(), "The exceptional initializer leaked resources acquired after close");
            assertNull(query.getClient());
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releaseInitializer.countDown();
        }
    }

    /**
     * A full page returned before its metric callback completed. Reaching maxResults on the next request terminated the generation, so that callback's later
     * publication was discarded even though its row had already been delivered by the same execution. This is normal completion, with no cancel or reset.
     *
     * Hold the callback while returning both the row and the final NONE page. All six counters must be accounted for before the final metric is sent, and
     * remain intact when the canceled producer's callback actually finishes.
     */
    @Test
    public void resultLimitCompletionPreservesDeliveredRowWorkMetrics() throws Exception {
        ExecutorService producer = executor();
        CountDownLatch writerEntered = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        MetricsTransformer transformer = new MetricsTransformer(7, 3) {
            @Override
            public void writeQueryMetrics(BaseQueryMetric metric) {
                writerEntered.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseWriter, 5, TimeUnit.SECONDS), "Metric callback was not released");
                super.writeQueryMetrics(metric);
            }
        };
        RunningQuery query = query(producer, Collections.<Object> singletonList("unused").iterator(), 1);
        query.getSettings().setMaxResultsOverridden(true);
        query.getSettings().setMaxResultsOverride(1L);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("delivered-row").iterator(), transformer));
        when(query.getLogic().getCollectQueryMetrics()).thenReturn(true);
        QueryMetricsBean metrics = mock(QueryMetricsBean.class);
        AtomicReference<List<Long>> finalPublishedCounters = new AtomicReference<>();
        doAnswer(invocation -> {
            BaseQueryMetric metric = invocation.getArgument(0);
            if (metric.getLifecycle() == BaseQueryMetric.Lifecycle.MAXRESULTS) {
                // Capture values when sent; retaining the mutable metric reference would hide a later correction.
                finalPublishedCounters.set(Arrays.asList(metric.getSourceCount(), metric.getNextCount(), metric.getSeekCount(), metric.getYieldCount(),
                                metric.getDocRanges(), metric.getFiRanges()));
            }
            return null;
        }).when(metrics).updateMetric(any());
        query.setQueryMetrics(metrics);
        query.setClient(mock(AccumuloClient.class));
        try {
            assertEquals(Collections.singletonList("delivered-row"), query.next().getResults());
            assertTrue(writerEntered.await(2, TimeUnit.SECONDS), "Metric callback did not block");
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
            assertWorkMetrics(query, 7, 3);
            assertEquals(Arrays.asList(10L, 7L, 3L, 10L, 10L, 10L), finalPublishedCounters.get(),
                            "The final metric was sent before accounting for its delivered row");
            releaseWriter.countDown();
            // Future cancellation is not task completion. This marker proves the callback finished on the same producer worker.
            producer.submit(() -> {}).get(2, TimeUnit.SECONDS);
            assertWorkMetrics(query, 7, 3);
        } finally {
            releaseWriter.countDown();
        }
    }

    /**
     * Reset terminated its producer and only later captured a generation and cleared cancellation state. A close/cancel completing inside the producer
     * cancellation callback was adopted as reset's generation, allowing that already-overtaken reset to install another client and reopen production.
     *
     * The Future callback is outside the lifecycle lock. Gate that real cancellation boundary rather than an internal reset method: termination must remain
     * responsive, and reset must retain its own generation even if cancellation completes slowly.
     *
     * @param closeQuery
     *            whether closeConnection() rather than cancel() overtakes reset
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    public void terminationDuringResetCancellationCannotBecomeTheResetGeneration(boolean closeQuery) throws Exception {
        ExecutorService producer = executor();
        CountDownLatch oldProducerBlocked = new CountDownLatch(1);
        CountDownLatch releaseOldProducer = new CountDownLatch(1);
        Iterator<Object> oldRows = new Iterator<>() {
            private int count;

            @Override
            public boolean hasNext() {
                if (count == 1) {
                    oldProducerBlocked.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseOldProducer, 5, TimeUnit.SECONDS), "Old producer was not released");
                }
                return count < 2;
            }

            @Override
            public Object next() {
                return count++ == 0 ? "old-row" : "old-prefetch";
            }
        };
        RunningQuery query = query(producer, oldRows, 1);
        assertEquals(Collections.singletonList("old-row"), query.next().getResults());
        assertTrue(oldProducerBlocked.await(2, TimeUnit.SECONDS), "Old producer did not block");
        Future<Object> handle = spy((Future<Object>) ReflectionTestUtils.getField(query, "future"));
        CountDownLatch cancelEntered = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            cancelEntered.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releaseCancel, 5, TimeUnit.SECONDS), "Producer cancellation was not released");
            return result;
        }).when(handle).cancel(true);
        ReflectionTestUtils.setField(query, "future", handle);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new-row").iterator(), value -> value));
        AccumuloClient oldClient = query.getClient();
        AccumuloClient replacement = mock(AccumuloClient.class);
        Future<?> initialization = executor().submit(() -> {
            query.setClient(replacement);
            return null;
        });
        try {
            assertTrue(cancelEntered.await(2, TimeUnit.SECONDS), "Reset did not cancel its producer");
            if (closeQuery) {
                query.closeConnection(mock(AccumuloConnectionFactory.class));
            } else {
                query.cancel();
            }
            releaseCancel.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> initialization.get(2, TimeUnit.SECONDS));
            assertInstanceOf(QueryException.class, failure.getCause(), "The overtaken initializer must fail before accepting its replacement client");
            if (closeQuery) {
                assertNull(query.getClient());
            } else {
                assertTrue(query.isCanceled(), "Reset cleared the cancellation that overtook it");
                assertSame(oldClient, query.getClient(), "Cancel does not transfer ownership to the replacement client");
            }
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());

            releaseOldProducer.countDown();
            producer.submit(() -> {}).get(2, TimeUnit.SECONDS);
            // Rejecting this attempt must still allow a later reset, after termination has finished, to start normally.
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            query.setClient(mock(AccumuloClient.class));
            assertEquals(Collections.singletonList("new-row"), query.next().getResults());
        } finally {
            releaseCancel.countDown();
            releaseOldProducer.countDown();
        }
    }

    /**
     * Prediction runs after reset but before the replacement client is installed. Close/cancel in that interval was observed only by the final producer guard:
     * setClient() still accepted the replacement client and initialized the logic. After close removed the query from the cache, that successful return left
     * its newly acquired resources outside normal cleanup.
     *
     * Reject the overtaken attempt before transferring client ownership, so the reset caller's existing failure path can return its unused pooled client.
     *
     * @param closeQuery
     *            whether closeConnection() rather than cancel() overtakes prediction
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void terminationDuringPredictionRejectsTheReplacementClient(boolean closeQuery) throws Exception {
        RunningQuery query = query(executor(), Collections.<Object> singletonList("old-row").iterator(), 1);
        CountDownLatch predicting = new CountDownLatch(1);
        CountDownLatch releasePredictor = new CountDownLatch(1);
        QueryPredictor<BaseQueryMetric> predictor = metric -> {
            predicting.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releasePredictor, 5, TimeUnit.SECONDS), "Predictor was not released");
            return Collections.emptySet();
        };
        query.setPredictor(predictor);
        AccumuloClient oldClient = query.getClient();
        AccumuloClient replacement = mock(AccumuloClient.class);
        // Distinguish replacement initialization from the fixture's already-completed initial setup.
        GenericQueryConfiguration replacementConfiguration = mock(GenericQueryConfiguration.class);
        when(query.getLogic().initialize(any(), any(), any())).thenReturn(replacementConfiguration);
        Future<?> initialization = executor().submit(() -> {
            query.setClient(replacement);
            return null;
        });
        try {
            assertTrue(predicting.await(2, TimeUnit.SECONDS), "Initialization did not enter prediction");
            if (closeQuery) {
                query.closeConnection(mock(AccumuloConnectionFactory.class));
            } else {
                query.cancel();
            }
            releasePredictor.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> initialization.get(2, TimeUnit.SECONDS));
            assertInstanceOf(QueryException.class, failure.getCause());
            verify(query.getLogic(), never()).setupQuery(replacementConfiguration);
            if (closeQuery) {
                assertNull(query.getClient(), "The closed initializer installed its replacement client");
            } else {
                assertSame(oldClient, query.getClient(), "The canceled initializer installed its replacement client");
            }
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releasePredictor.countDown();
        }
    }

    /**
     * Failed-return recovery must retain an older lease without attaching it to a newer initialization. A client-null check alone is insufficient: prediction
     * records a replacement attempt before installing its client. Hold the older return across either that prediction window or a completed replacement.
     *
     * These are passing controls for ownership-based recovery. The replacement must keep its own client/iterator, while the older caller can separately claim
     * and return its failed loan. No failed-return recovery may install the old client during the replacement's prediction.
     *
     * @param pendingPrediction
     *            whether the replacement is still predicting rather than already installed when the old return fails
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void failedOldCloseDoesNotOverwriteReplacementInitialization(boolean pendingPrediction) throws Exception {
        RunningQuery query = query(executor(), Collections.<Object> singletonList("old-row").iterator(), 1);
        AccumuloClient oldClient = query.getClient();
        AccumuloClient replacement = mock(AccumuloClient.class);
        AccumuloConnectionFactory factory = mock(AccumuloConnectionFactory.class);
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch releaseReturn = new CountDownLatch(1);
        CountDownLatch predicting = new CountDownLatch(1);
        CountDownLatch releasePredictor = new CountDownLatch(1);
        IOException originalFailure = new IOException("Old close failed during replacement initialization");
        AtomicBoolean firstReturn = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (firstReturn.compareAndSet(true, false)) {
                returning.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Old return was not released");
                throw originalFailure;
            }
            return null;
        }).when(factory).returnClient(oldClient);
        ExecutorService requests = executor(2);
        Future<?> close = requests.submit(() -> {
            query.closeConnection(factory);
            return null;
        });
        try {
            assertTrue(returning.await(2, TimeUnit.SECONDS), "Old close did not reach return");
            when(query.getLogic().getTransformIterator(query.getSettings()))
                            .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new-row").iterator(), value -> value));
            Future<?> initialization = null;
            if (pendingPrediction) {
                query.setPredictor(metric -> {
                    predicting.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releasePredictor, 5, TimeUnit.SECONDS), "Replacement predictor was not released");
                    return Collections.emptySet();
                });
                initialization = requests.submit(() -> {
                    query.setClient(replacement);
                    return null;
                });
                assertTrue(predicting.await(2, TimeUnit.SECONDS), "Replacement did not enter prediction");
                assertNull(query.getClient(), "Prediction must precede replacement installation");
            } else {
                query.setClient(replacement);
            }
            releaseReturn.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> close.get(2, TimeUnit.SECONDS));
            assertSame(originalFailure, failure.getCause());
            if (pendingPrediction) {
                assertNull(query.getClient(), "Failed old close installed its client into a newer prediction");
                releasePredictor.countDown();
                initialization.get(2, TimeUnit.SECONDS);
            }
            assertSame(replacement, query.getClient(), "Failed old close overwrote the replacement lease");
            assertSame(oldClient, query.releaseClientOnInitializationFailure(oldClient), "The older caller could not claim its failed loan");
            factory.returnClient(oldClient);
            assertSame(replacement, query.getClient(), "Releasing the old loan detached the replacement");
            assertEquals(Collections.singletonList("new-row"), query.next().getResults());
        } finally {
            releaseReturn.countDown();
            releasePredictor.countDown();
        }
    }

    /**
     * Async pages checked maxWork before hasNext(), but the producer could publish enough work to exhaust that budget while the request waited. The request
     * then consumed another row without rechecking the updated counters. Keep the first metric callback blocked after returning a one-row page, wait for the
     * second request to need another row, and only then publish the exhausted budget.
     *
     * @param seekBudget
     *            whether seekCount rather than nextCount reaches maxWork during the wait
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void workBudgetPublishedDuringHasNextWaitStopsThePage(boolean seekBudget) throws Exception {
        ExecutorService producer = executor();
        CountDownLatch writerEntered = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        AtomicBoolean firstWrite = new AtomicBoolean(true);
        MetricsTransformer transformer = new MetricsTransformer(seekBudget ? 0 : 100, seekBudget ? 100 : 0) {
            @Override
            public void writeQueryMetrics(BaseQueryMetric metric) {
                if (firstWrite.compareAndSet(true, false)) {
                    writerEntered.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseWriter, 5, TimeUnit.SECONDS), "Metric writer was not released");
                }
                super.writeQueryMetrics(metric);
            }
        };
        RunningQuery query = query(producer, Collections.<Object> singletonList("unused").iterator(), 1);
        when(query.getLogic().getMaxWork()).thenReturn(100L);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Arrays.<Object> asList("a", "b", "c").iterator(), transformer));
        query.setClient(mock(AccumuloClient.class));
        try {
            assertEquals(Collections.singletonList("a"), query.next().getResults());
            assertTrue(writerEntered.await(2, TimeUnit.SECONDS), "Metric writer did not block");
            assertWorkMetrics(query, 0, 0);
            Future<ResultsPage> page = executor().submit(query::next);
            awaitResultWait(query, "hasNext");
            releaseWriter.countDown();
            ResultsPage result = page.get(2, TimeUnit.SECONDS);
            assertEquals(ResultsPage.Status.NONE, result.getStatus(), "The page consumed another row after its work budget was exhausted");
            assertTrue(result.getResults().isEmpty());
            producer.submit(() -> {}).get(2, TimeUnit.SECONDS);
            assertWorkMetrics(query, seekBudget ? 0 : 100, seekBudget ? 100 : 0);
            assertEquals(BaseQueryMetric.Lifecycle.MAXWORK, query.getMetric().getLifecycle());
        } finally {
            releaseWriter.countDown();
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

    /**
     * Async maxWork was checked after hasNext but not after getNext's separate wait. Publish the exhausted next/seek budget while that latter wait returns a
     * row: output must match the control in which the same work is published before the next page starts. The queue gate allows the real producer to publish
     * its counters before the request resumes; the test never mutates the live metric itself. Hold the second metric callback until the poll starts so stronger
     * budget checks inside the wait cannot bypass this separate post-getNext boundary.
     *
     * @param seekBudget
     *            whether seekCount rather than nextCount reaches maxWork
     * @param publishBeforeRequest
     *            whether to run the already-exhausted-budget control
     */
    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    public void workBudgetPublishedDuringGetNextWaitStopsThePage(boolean seekBudget, boolean publishBeforeRequest) throws Exception {
        CountDownLatch nextBlocked = new CountDownLatch(1);
        CountDownLatch releaseNext = new CountDownLatch(1);
        CountDownLatch pollBlocked = new CountDownLatch(1);
        CountDownLatch releasePoll = new CountDownLatch(1);
        CountDownLatch releaseMetrics = new CountDownLatch(1);
        AtomicInteger index = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return index.get() < 2;
            }

            @Override
            public Object next() {
                if (index.getAndIncrement() == 0) {
                    return "a";
                }
                nextBlocked.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseNext, 5, TimeUnit.SECONDS), "Second iterator next was not released");
                return "b";
            }
        };
        MetricsTransformer transformer = new MetricsTransformer(0, 0) {
            @Override
            public Object transform(Object value) {
                long work = "a".equals(value) ? 1 : 100;
                setWorkCounts(seekBudget ? 0 : work, seekBudget ? work : 0);
                return value;
            }

            @Override
            public void writeQueryMetrics(BaseQueryMetric metric) {
                if (!publishBeforeRequest && getSourceCount() == 100) {
                    // Keep wait-loop checks below the limit, then publish only after the real poll has returned its row.
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseMetrics, 5, TimeUnit.SECONDS), "Second metric publication was not released");
                }
                super.writeQueryMetrics(metric);
            }
        };
        RunningQuery query = query(executor(), Collections.<Object> singletonList("unused").iterator(), 1);
        when(query.getLogic().getMaxWork()).thenReturn(100L);
        when(query.getLogic().getTransformIterator(query.getSettings())).thenReturn(new TransformIterator<>(rows, transformer));
        query.setClient(mock(AccumuloClient.class));
        BlockingQueue<Object> queue = spy((BlockingQueue<Object>) ReflectionTestUtils.getField(query, "resultsThreadQueue"));
        ReflectionTestUtils.setField(query, "resultsThreadQueue", queue);
        AtomicInteger polls = new AtomicInteger();
        doAnswer(invocation -> {
            Object value = invocation.callRealMethod();
            if (polls.incrementAndGet() == 2) {
                pollBlocked.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releasePoll, 5, TimeUnit.SECONDS), "Second queue poll was not released");
            }
            return value;
        }).when(queue).poll();
        try {
            assertEquals(Collections.singletonList("a"), query.next().getResults());
            assertTrue(nextBlocked.await(2, TimeUnit.SECONDS), "Producer did not block in second next()");
            Future<ResultsPage> second = null;
            if (!publishBeforeRequest) {
                second = executor().submit(query::next);
                awaitResultWait(query, "getNext");
            }
            releaseNext.countDown();
            if (!publishBeforeRequest) {
                assertTrue(pollBlocked.await(2, TimeUnit.SECONDS), "Request did not reach its second queue poll");
            }
            releaseMetrics.countDown();
            Object generationLock = ReflectionTestUtils.getField(query, "running");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            long published;
            do {
                synchronized (generationLock) {
                    published = query.getMetric().getNextCount() + query.getMetric().getSeekCount();
                }
                if (published >= 100) {
                    break;
                }
                Thread.yield();
            } while (System.nanoTime() < deadline);
            assertEquals(100, published, "Producer did not publish the exhausted budget before the request resumed");
            releasePoll.countDown();
            ResultsPage result = publishBeforeRequest ? query.next() : second.get(2, TimeUnit.SECONDS);
            assertEquals(ResultsPage.Status.NONE, result.getStatus(), "The exhausted budget must stop output regardless of publication timing");
            assertTrue(result.getResults().isEmpty());
            assertEquals(BaseQueryMetric.Lifecycle.MAXWORK, query.getMetric().getLifecycle());
            assertEquals(1L, ReflectionTestUtils.getField(query, "numResults"), "The over-budget row must not be included in page accounting");
        } finally {
            releaseNext.countDown();
            releaseMetrics.countDown();
            releasePoll.countDown();
        }
    }

    /**
     * Work published by a filtered row exhausted maxWork but supplied no queued result to finish getNext. The request waited for more backend work and timed
     * out instead of returning normal work-limit completion. Publish next/seek exhaustion while the request waits, then hold the later backend hasNext call.
     * The one-second Future deadline is below the normal ten-second page deadline: only budget handling can complete the request while that backend is held.
     * Controls publish before requesting the next page; nonempty-page cases also preserve the row already collected in the same request.
     *
     * @param seekBudget
     *            whether seekCount rather than nextCount exhausts the budget
     * @param publishBeforeRequest
     *            whether to run the already-exhausted-budget control
     * @param samePage
     *            whether the first row belongs to the waiting request rather than its preceding page
     */
    @ParameterizedTest
    @CsvSource({"false, false, false", "true, false, false", "false, true, false", "true, true, false", "false, false, true", "true, false, true"})
    public void filteredRowWorkExhaustionCompletesAWaitingPage(boolean seekBudget, boolean publishBeforeRequest, boolean samePage) throws Exception {
        CountDownLatch filteredBlocked = new CountDownLatch(1);
        CountDownLatch releaseFiltered = new CountDownLatch(1);
        CountDownLatch backendBlocked = new CountDownLatch(1);
        CountDownLatch releaseBackend = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (calls.get() < 2) {
                    return true;
                }
                backendBlocked.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseBackend, 5, TimeUnit.SECONDS), "Post-budget backend wait was not released");
                return false;
            }

            @Override
            public Object next() {
                if (calls.incrementAndGet() == 1) {
                    return "first";
                }
                filteredBlocked.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseFiltered, 5, TimeUnit.SECONDS), "Filtered row was not released");
                return "filtered";
            }
        };
        MetricsTransformer transformer = new MetricsTransformer(0, 0) {
            @Override
            public Object transform(Object value) {
                long work = "first".equals(value) ? 1 : 100;
                setWorkCounts(seekBudget ? 0 : work, seekBudget ? work : 0);
                return "filtered".equals(value) ? null : value;
            }
        };
        RunningQuery query = query(executor(), Collections.<Object> singletonList("unused").iterator(), samePage ? 2 : 1);
        when(query.getLogic().getMaxWork()).thenReturn(100L);
        when(query.getLogic().getTransformIterator(query.getSettings())).thenReturn(new TransformIterator<>(rows, transformer));
        query.setClient(mock(AccumuloClient.class));
        try {
            Future<ResultsPage> page = samePage ? executor().submit(query::next) : null;
            if (!samePage) {
                assertEquals(Collections.singletonList("first"), query.next().getResults());
            }
            assertTrue(filteredBlocked.await(2, TimeUnit.SECONDS));
            if (!publishBeforeRequest) {
                if (!samePage) {
                    page = executor().submit(query::next);
                }
                awaitResultWait(query, "getNext");
            }
            releaseFiltered.countDown();
            if (publishBeforeRequest) {
                // No request is active in this control. Reaching backend work proves publication completed before next starts.
                assertTrue(backendBlocked.await(2, TimeUnit.SECONDS), "Producer did not publish work before the control request");
            }
            Future<ResultsPage> waitingPage = page;
            ResultsPage result = publishBeforeRequest ? query.next()
                            : assertDoesNotThrow(() -> waitingPage.get(1, TimeUnit.SECONDS),
                                            "Published work exhaustion must finish the wait without another row");
            // A fixed waiting request may stop the producer before another backend call begins; completion itself proves it observed publication.
            Object generationLock = ReflectionTestUtils.getField(query, "running");
            synchronized (generationLock) {
                assertWorkMetrics(query, seekBudget ? 0 : 100, seekBudget ? 100 : 0);
            }
            if (samePage) {
                assertEquals(Collections.singletonList("first"), result.getResults());
                result = query.next();
            }
            assertEquals(ResultsPage.Status.NONE, result.getStatus());
            assertTrue(result.getResults().isEmpty());
            assertEquals(BaseQueryMetric.Lifecycle.MAXWORK, query.getMetric().getLifecycle());
            assertEquals(1L, ReflectionTestUtils.getField(query, "numResults"));
        } finally {
            releaseFiltered.countDown();
            releaseBackend.countDown();
        }
    }

    /**
     * Work exhaustion during a request containing one row returned that row but only empty pages completed the producer. With no subsequent next call, filtered
     * rows kept the queue empty, so the worker continued consuming backend work beyond the acknowledged budget. RESULTS also overwrote MAXWORK. Hold the second
     * transformed row until the caller is waiting, publish next/seek exhaustion, and inspect the returned page before requesting anything else. Release any
     * already-started backend call and run a sentinel on the same executor: its completion gives a deterministic final row count. An immediate final next is
     * the former cleanup control; it stopped the extra work even before the nonempty-page fix.
     */
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    public void nonemptyWorkLimitedPageStopsBackendProductionWithoutAnotherRequest(boolean seekBudget, boolean terminalNext) throws Exception {
        CountDownLatch filtered = new CountDownLatch(1);
        CountDownLatch releaseFiltered = new CountDownLatch(1);
        CountDownLatch releaseBackend = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (calls.get() == 2) {
                    // One hasNext may already be in flight when the request observes exhausted work. It must not admit subsequent next calls.
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseBackend, 5, TimeUnit.SECONDS), "Backend call was not released");
                }
                return calls.get() < 8;
            }

            @Override
            public Object next() {
                if (calls.incrementAndGet() == 1) {
                    return "first";
                }
                if (calls.get() == 2) {
                    filtered.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseFiltered, 5, TimeUnit.SECONDS), "Filtered row was not released");
                }
                return "filtered";
            }
        };
        MetricsTransformer transformer = new MetricsTransformer(0, 0) {
            private long work;

            @Override
            public Object transform(Object value) {
                work = "first".equals(value) ? 1 : Math.max(100, work + 1);
                setWorkCounts(seekBudget ? 0 : work, seekBudget ? work : 0);
                return "first".equals(value) ? value : null;
            }
        };
        ExecutorService producer = executor();
        RunningQuery query = query(producer, Collections.<Object> singletonList("unused").iterator(), 2);
        when(query.getLogic().getMaxWork()).thenReturn(100L);
        when(query.getLogic().getTransformIterator(query.getSettings())).thenReturn(new TransformIterator<>(rows, transformer));
        query.setClient(mock(AccumuloClient.class));
        try {
            Future<ResultsPage> page = executor().submit(query::next);
            assertTrue(filtered.await(2, TimeUnit.SECONDS));
            awaitResultWait(query, "getNext");
            releaseFiltered.countDown();
            ResultsPage result = assertDoesNotThrow(() -> page.get(1, TimeUnit.SECONDS));
            assertEquals(Collections.singletonList("first"), result.getResults());
            BaseQueryMetric.Lifecycle returnedLifecycle = query.getMetric().getLifecycle();
            if (terminalNext) {
                assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
            }
            releaseBackend.countDown();
            producer.submit(() -> {}).get(2, TimeUnit.SECONDS);
            assertEquals(2, calls.get(), "The producer consumed further filtered rows after returning its work-limited page");
            assertWorkMetrics(query, seekBudget ? 0 : 100, seekBudget ? 100 : 0);
            assertEquals(BaseQueryMetric.Lifecycle.MAXWORK, query.getMetric().getLifecycle());
            if (!terminalNext) {
                assertEquals(BaseQueryMetric.Lifecycle.MAXWORK, returnedLifecycle, "Returning collected results hid the work cutoff");
                assertTrue(query.isFinished(), "Work-limited completion must be visible without another client request");
                assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
            }
            assertEquals(1L, ReflectionTestUtils.getField(query, "numResults"));
        } finally {
            releaseFiltered.countDown();
            releaseBackend.countDown();
        }
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
