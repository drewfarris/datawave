package datawave.webservice.query.runner;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.jboss.logging.NDC;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.core.query.logic.WritesResultCardinalities;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.authorization.remote.RemoteUserOperationsImpl;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.common.connection.WrappedAccumuloClient;
import datawave.webservice.query.cache.AbstractRunningQuery;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

/**
 * Object that encapsulates a running query
 *
 */
public class RunningQuery extends AbstractRunningQuery implements Runnable {

    private static final long serialVersionUID = -8812242917762274469L;

    /**
     * Keep a small, bounded number of transformed results ready for the request thread. A capacity of one effectively serializes the producer and consumer and
     * used to require the producer to poll the queue every millisecond. Keeping this bounded limits the amount of result data retained per running query.
     */
    private static final int RESULTS_THREAD_QUEUE_CAPACITY = 2;

    /**
     * A producer can finish before a bounded executor has returned its worker to the available pool. Retry a rejected submission only when another result is
     * needed, with a short wait bounded by the page deadline.
     */
    private static final long RESULTS_THREAD_RETRY_WAIT_MS = 10;

    private static Logger log = Logger.getLogger(RunningQuery.class);

    private transient AccumuloClient client = null;
    private AccumuloConnectionFactory.Priority connectionPriority = null;
    private transient QueryLogic<?> logic = null;
    private Query settings = null;
    private volatile int currentPageCount = 0;
    private long numResults = 0;
    private volatile AtomicLong lastPageNumber = new AtomicLong(0);
    private volatile transient TransformIterator iter = null;
    private Set<Authorizations> calculatedAuths = null;
    private volatile boolean finished = false;
    private volatile boolean canceled = false;
    private transient QueryMetricsBean queryMetrics = null;
    private transient RunningQueryTiming timing = null;
    private ExecutorService executor = null;
    private volatile Future<Object> future = null;
    private final BlockingQueue<Object> resultsThreadQueue = new ArrayBlockingQueue<>(RESULTS_THREAD_QUEUE_CAPACITY);
    private transient volatile Thread currentThread;
    private volatile Exception resultsThreadException = null;
    private final AtomicInteger hasNext = new AtomicInteger(0);
    private final AtomicInteger gotNext = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean resultsThreadFinished = false;
    // Termination is distinct from yielding a worker between pages; only successful initialization reopens an execution.
    private volatile boolean resultsThreadStopped = true;
    private final AtomicLong resultsThreadGeneration = new AtomicLong(0);
    // Guarded by running. Producers capture this counter buffer so a callback that outlives reset cannot write into the live metric.
    private transient BaseQueryMetric resultsThreadMetric;
    // Guarded by running; unlike the externally managed activeCall, this tracks actual page consumption.
    private boolean pageInProgress = false;
    private final AtomicBoolean forcedReturn = new AtomicBoolean(false);
    private QueryPredictor predictor = null;
    private long maxResults = 0;
    private int currentTimeoutcount = 0;
    private boolean allowIntermediateEmptyPages = false;
    private boolean useResultsThread = true;

    public RunningQuery() {
        super(new QueryMetricFactoryImpl());
    }

    public RunningQuery(AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings, String methodAuths,
                    Principal principal, QueryMetricFactory metricFactory) throws Exception {
        this(null, client, priority, logic, settings, methodAuths, principal, null, null, metricFactory);
    }

    public RunningQuery(AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings, String methodAuths,
                    Principal principal, RunningQueryTiming timing, QueryMetricFactory metricFactory) throws Exception {
        this(null, client, priority, logic, settings, methodAuths, principal, timing, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, QueryMetricFactory metricFactory) throws Exception {
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryMetricFactory metricFactory) throws Exception {
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, timing, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryPredictor predictor, QueryMetricFactory metricFactory)
                    throws Exception {
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, timing, predictor, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryPredictor predictor, UserOperations userOperations,
                    QueryMetricFactory metricFactory) throws Exception {
        super(metricFactory);
        if (logic != null && logic.getCollectQueryMetrics()) {
            this.queryMetrics = queryMetrics;
        }
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
        this.logic = logic;
        this.connectionPriority = priority;
        this.settings = settings;
        // the query principal is our local principal unless the query logic has a different user operations
        if (methodAuths != null) {
            logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(WSAuthorizationsUtil.splitAuths(methodAuths))));
        } else {
            logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(null));
        }
        DatawavePrincipal queryPrincipal = (DatawavePrincipal) ((logic.getUserOperations() == null) ? principal
                        : logic.getUserOperations().getRemoteUser((DatawavePrincipal) principal));
        // the overall principal (the one with combined auths across remote user operations) is our own user operations (probably the UserOperationsBean)
        // don't call remote user operations if it's asked not to
        DatawavePrincipal overallPrincipal = (userOperations == null
                        || "false".equalsIgnoreCase(settings.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES).getParameterValue()))
                                        ? (DatawavePrincipal) principal
                                        : userOperations.getRemoteUser((DatawavePrincipal) principal);
        this.calculatedAuths = WSAuthorizationsUtil.getDowngradedAuthorizations(methodAuths, overallPrincipal, queryPrincipal);
        this.timing = timing;
        this.predictor = predictor;
        // set the metric information
        this.getMetric().populate(this.settings);
        this.getMetric().setQueryType(this.getClass().getSimpleName());
        if (this.queryMetrics != null) {
            try {
                this.queryMetrics.updateMetric(this.getMetric());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        // If connection is null, then we are likely not going to use this object for query, probably for removing or closing it.
        if (null != client) {
            setClient(client);
        }

        this.maxResults = this.logic.getResultLimit(this.settings);
        if (this.maxResults != this.logic.getMaxResults()) {
            log.info("Maximum results set to " + this.maxResults + " instead of default " + this.logic.getMaxResults() + ", user " + this.settings.getUserDN()
                            + " has a DN configured with a different limit");
        }
    }

    public static RunningQuery createQueryWithAuthorizations(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority,
                    QueryLogic<?> logic, Query settings, String methodAuths, RunningQueryTiming timing, QueryPredictor predictor,
                    QueryMetricFactory metricFactory) throws Exception {
        RunningQuery runningQuery = new RunningQuery(queryMetrics, client, priority, logic, settings, methodAuths, null, timing, predictor, metricFactory);
        runningQuery.calculatedAuths = Collections.singleton(new Authorizations(methodAuths));
        return runningQuery;
    }

    private void addNDC() {
        String user = this.settings.getUserDN();
        UUID uuid = this.settings.getId();
        if (user != null && uuid != null) {
            NDC.push("[" + user + "] [" + uuid + "]");
        }
    }

    private void removeNDC() {
        NDC.pop();
    }

    public void setClient(AccumuloClient client) throws Exception {
        // if we are setting this null, we shouldn't try to initialize
        // the internal logic
        if (client == null) {
            this.client = null;
            return;
        }

        try {
            addNDC();
            long generation = resetResultsThreadState();
            applyPrediction(null);
            synchronized (running) {
                // Reject before accepting the pooled client: the caller still owns it and can return it on failure.
                if (generation != resultsThreadGeneration.get() || canceled) {
                    throw new QueryException(DatawaveErrorCode.QUERY_CANCELED, "Initialization was terminated before its client was installed");
                }
                this.client = client;
            }
            if (this.client instanceof WrappedAccumuloClient && this.logic.getClientConfig() != null) {
                ((WrappedAccumuloClient) this.client).updateClientConfig(this.logic.getClientConfig());
            }
            long start = System.currentTimeMillis();
            GenericQueryConfiguration configuration = this.logic.initialize(this.client, this.settings, this.calculatedAuths);
            if (!isCurrentInitialization(generation)) {
                // Close may have returned the installed client while initialize() was still acquiring planning resources.
                // Reclaim those late resources without accepting another client or running setup after termination.
                closeLogic();
                return;
            }
            this.lastPageNumber.set(0);
            this.logic.setupQuery(configuration);
            this.iter = this.logic.getTransformIterator(this.settings);
            this.allowIntermediateEmptyPages = logic.isIntermediateEmptyPagesEnabled();
            // force us to use asynchronous results thread to allow intermediate empty pages
            if (this.allowIntermediateEmptyPages) {
                this.useResultsThread = true;
            } else {
                // force us to not use asynchronous results thread if synchronous is requested
                if (logic.isUseSynchronousRunningQuery()) {
                    this.useResultsThread = false;
                }
            }
            // the configuration query string should now hold the initial planned query
            this.getMetric().setPlan(configuration.getQueryString());
            this.getMetric().setSetupTime((System.currentTimeMillis() - start));
            this.getMetric().setLifecycle(QueryMetric.Lifecycle.INITIALIZED);
            testForUncaughtException(0);
            // TODO: applyPrediction("Plan");
            synchronized (running) {
                if (generation == resultsThreadGeneration.get() && !canceled) {
                    resultsThreadStopped = false;
                    return;
                }
            }
            // setupQuery() and transformer creation can also finish after close has already cleaned up the logic.
            closeLogic();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (this.logic instanceof BaseQueryLogic && this.logic.getCollectQueryMetrics() && null != this.getMetric()) {
                GenericQueryConfiguration config = ((BaseQueryLogic) this.logic).getConfig();
                if (null != config) {
                    this.getMetric().setPlan(config.getQueryString());
                }
            }
            this.getMetric().setError(e);
            throw e;
        } finally {
            // update AbstractRunningQuery.lastUsed in case this operation took a long time
            touch();
            removeNDC();
            if (this.queryMetrics != null) {
                try {
                    this.queryMetrics.updateMetric(this.getMetric());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    private boolean isCurrentInitialization(long generation) {
        synchronized (running) {
            return generation == resultsThreadGeneration.get() && !canceled;
        }
    }

    private long resetResultsThreadState() {
        Future<Object> resultsFuture;
        long generation;
        synchronized (running) {
            // Capture this reset's generation and clear state in the same transition that stops its old producer.
            // Cancellation callbacks run later, so a concurrent close/cancel cannot become this reset's token.
            resultsFuture = stopResultsThreadState();
            generation = resultsThreadGeneration.get();
            resultsThreadQueue.clear();
            hasNext.set(0);
            gotNext.set(0);
            resultsThreadException = null;
            resultsThreadFinished = false;
            finished = false;
            canceled = false;
            numResults = 0;
            currentTimeoutcount = 0;
            currentThread = null;
            pageInProgress = false;
            // The replacement transformer starts with fresh work counters. Reset the live budget before next() can enforce maxWork.
            resetWorkMetrics(this.getMetric());
            resultsThreadMetric = new QueryMetric();
            if (iter != null && settings.getUncaughtExceptionHandler() != null) {
                // Renew the handler before initialization gives it to new scanner/planner threads.
                // Threads retaining the previous handler cannot report failures into this execution.
                settings.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
            }
        }
        cancelResultsFuture(resultsFuture);
        synchronized (forcedReturn) {
            currentPageCount = 0;
            forcedReturn.set(false);
        }
        return generation;
    }

    private void startResultsThread() {
        synchronized (running) {
            if (useResultsThread && executor != null && !running.get() && !resultsThreadFinished && !resultsThreadStopped && resultsThreadException == null
                            && !finished && !canceled && !forcedReturn.get() && resultsThreadQueue.remainingCapacity() > 0) {
                long generation = resultsThreadGeneration.get();
                TransformIterator<?,?> iterator = iter;
                BaseQueryMetric workMetric = resultsThreadMetric;
                running.set(true);
                try {
                    future = executor.submit(() -> getResultsThread(generation, iterator, workMetric));
                } catch (RuntimeException e) {
                    running.set(false);
                    throw e;
                }
            }
        }
    }

    private boolean isCurrentResultsThread(long generation) {
        return generation == resultsThreadGeneration.get() && running.get() && !resultsThreadStopped && !finished && !canceled;
    }

    /**
     * This is the results thread which will pull results from the iterator and add them to a bounded blocking queue. The queue allows a small amount of
     * read-ahead. While a page is being consumed, the producer waits for queue capacity instead of repeatedly resubmitting itself. Between requests, a full
     * queue returns the worker to the executor. The hasNext and gotNext counters track calls on the underlying iterator and are decremented once a result is
     * acknowledged in RunningQuery.next().
     *
     * @param generation
     *            the execution to which this producer belongs
     * @param iterator
     *            the iterator captured for this execution
     * @param workMetric
     *            the metric buffer captured for this execution
     * @return false once the producer has yielded or finished
     */
    private Object getResultsThread(long generation, TransformIterator<?,?> iterator, BaseQueryMetric workMetric) {
        try {
            while (isCurrentResultsThread(generation)) {
                synchronized (running) {
                    while (isCurrentResultsThread(generation) && resultsThreadQueue.remainingCapacity() == 0 && pageInProgress && !forcedReturn.get()) {
                        running.wait();
                    }
                    if (!isCurrentResultsThread(generation) || resultsThreadQueue.remainingCapacity() == 0) {
                        break;
                    }
                }
                if (!iterator.hasNext()) {
                    synchronized (running) {
                        if (isCurrentResultsThread(generation)) {
                            resultsThreadFinished = true;
                        }
                    }
                    break;
                }
                synchronized (running) {
                    if (!isCurrentResultsThread(generation)) {
                        break;
                    }
                    hasNext.incrementAndGet();
                }
                synchronized (hasNext) {
                    hasNext.notifyAll();
                }

                Object o = iterator.next();
                synchronized (running) {
                    // A canceled producer may return after reset has installed a new iterator.
                    // Its result and counters must not enter the new execution.
                    if (!isCurrentResultsThread(generation)) {
                        break;
                    }
                    if (o != null) {
                        resultsThreadQueue.add(o);
                        gotNext.incrementAndGet();
                    } else {
                        hasNext.decrementAndGet();
                    }
                }
                signalResultsThreadWaiters();

                // The iterator may update metrics even when it produces no transformed result.
                if (isCurrentResultsThread(generation) && iterator.getTransformer() instanceof WritesQueryMetrics) {
                    // Callbacks may outlive cancellation. Keep their writes off the live metric and publish only under the generation lock.
                    ((WritesQueryMetrics) iterator.getTransformer()).writeQueryMetrics(workMetric);
                    synchronized (running) {
                        if (isCurrentResultsThread(generation)) {
                            publishWorkMetrics(workMetric);
                        }
                    }
                }
            }
        } catch (Exception e) {
            synchronized (running) {
                if (isCurrentResultsThread(generation)) {
                    recordResultsThreadException(e);
                }
            }
        } finally {
            synchronized (running) {
                if (generation == resultsThreadGeneration.get()) {
                    running.set(false);
                    future = null;
                }
            }
            signalResultsThreadWaiters();
        }

        return false;
    }

    private void resetWorkMetrics(BaseQueryMetric metric) {
        metric.setSourceCount(0);
        metric.setNextCount(0);
        metric.setSeekCount(0);
        metric.setYieldCount(0);
        metric.setDocRanges(0);
        metric.setFiRanges(0);
    }

    private void publishWorkMetrics(BaseQueryMetric workMetric) {
        BaseQueryMetric metric = this.getMetric();
        metric.setSourceCount(workMetric.getSourceCount());
        metric.setNextCount(workMetric.getNextCount());
        metric.setSeekCount(workMetric.getSeekCount());
        metric.setYieldCount(workMetric.getYieldCount());
        metric.setDocRanges(workMetric.getDocRanges());
        metric.setFiRanges(workMetric.getFiRanges());
    }

    private void recordResultsThreadException(Exception e) {
        log.error(this.settings.getId() + " : Results thread exception", e);
        resultsThreadException = e;
        if (settings.getUncaughtExceptionHandler() != null) {
            settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
        }
    }

    private void signalResultsThreadWaiters() {
        synchronized (hasNext) {
            hasNext.notifyAll();
        }
        synchronized (gotNext) {
            gotNext.notifyAll();
        }
    }

    public boolean isUseResultsThread() {
        return useResultsThread;
    }

    public void setUseResultsThread(boolean useResultsThread) {
        this.useResultsThread = useResultsThread;
    }

    /**
     * This method is used to determine if we have a next result. This will throw a timeout exception if the page short circuit limit is reached.
     *
     * @param pageStartTime
     *            the page start time
     * @return true if hasNext()
     * @throws TimeoutException
     *             if there is a timeout
     */
    private boolean hasNext(long pageStartTime) throws TimeoutException {
        if (useResultsThread && executor != null) {
            return waitForResults(hasNext, pageStartTime);
        } else {
            if (!this.finished && !this.canceled && this.iter.hasNext()) {
                hasNext.incrementAndGet();
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * This method will get the next object from the results thread queue. This presumes that hasNext has returned true. A timeout exception will be thrown if
     * the page short circuit timeout has been reached.
     *
     * @param pageStartTime
     *            the page start time
     * @return the next object (could be null)
     * @throws TimeoutException
     *             if there is a timeout
     */
    private Object getNext(long pageStartTime) throws TimeoutException {
        if (useResultsThread && executor != null) {
            if (!waitForResults(gotNext, pageStartTime)) {
                return null;
            }
            Object result;
            synchronized (gotNext) {
                throwResultsThreadException();
                if (forcedReturn.get()) {
                    throw new TimeoutException("Query page return was forced");
                }
                result = resultsThreadQueue.poll();
            }
            synchronized (running) {
                running.notifyAll();
            }
            return result;
        } else {
            Object o = iter.next();
            gotNext.incrementAndGet();

            // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
            if (iter.getTransformer() instanceof WritesQueryMetrics) {
                ((WritesQueryMetrics) iter.getTransformer()).writeQueryMetrics(this.getMetric());
            }

            return o;
        }
    }

    private boolean waitForResults(AtomicInteger available, long pageStartTime) throws TimeoutException {
        long generation = resultsThreadGeneration.get();
        while (true) {
            synchronized (available) {
                throwResultsThreadException();
                if (forcedReturn.get() && (available == gotNext || available.get() == 0)) {
                    throw new TimeoutException("Query page return was forced");
                }
                if (generation != resultsThreadGeneration.get() || finished || canceled) {
                    return false;
                }
                if (available.get() > 0) {
                    return true;
                }
                if (resultsThreadFinished || resultsThreadStopped) {
                    return false;
                }
                getResultsWaitTimeout(pageStartTime);
            }

            // Submit outside the wait monitor: the producer publishes state under running,
            // then notifies the result monitors after releasing that lock.
            boolean rejected = false;
            try {
                synchronized (running) {
                    // Termination must not restart the iterator belonging to this request.
                    if (generation != resultsThreadGeneration.get()) {
                        return false;
                    }
                    startResultsThread();
                }
            } catch (RejectedExecutionException e) {
                if (executor.isShutdown()) {
                    throw e;
                }
                rejected = true;
            }
            synchronized (available) {
                throwResultsThreadException();
                if (generation != resultsThreadGeneration.get()) {
                    return false;
                }
                if (available.get() == 0 && !finished && !canceled && !resultsThreadFinished && !resultsThreadStopped) {
                    long timeout = getResultsWaitTimeout(pageStartTime);
                    if (rejected || !running.get()) {
                        // A yielded producer is not iterator exhaustion. Wait briefly before
                        // retrying; a successful submission can still race executor handoff.
                        timeout = Math.min(timeout, RESULTS_THREAD_RETRY_WAIT_MS);
                    }
                    try {
                        available.wait(timeout);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }
    }

    private long getResultsWaitTimeout(long pageStartTime) throws TimeoutException {
        // The caller holds the wait monitor, so a force notification cannot be lost
        // between checking the flag and starting the wait.
        if (forcedReturn.get()) {
            throw new TimeoutException("Query page return was forced");
        }
        long timeout = timing != null ? timing.getPageShortCircuitTimeoutMs() - (System.currentTimeMillis() - pageStartTime) : Long.MAX_VALUE;
        if (timeout <= 0) {
            throw new TimeoutException("Query page timed out waiting for a result");
        }
        return timeout;
    }

    private void throwResultsThreadException() {
        if (resultsThreadException != null) {
            if (resultsThreadException instanceof RuntimeException) {
                throw (RuntimeException) resultsThreadException;
            }
            throw new RuntimeException(resultsThreadException);
        }
    }

    /**
     * terminate the results thread.
     */
    public void terminateResultsThread() {
        Future<Object> resultsFuture;
        synchronized (running) {
            resultsFuture = stopResultsThreadState();
        }
        cancelResultsFuture(resultsFuture);
    }

    // Caller holds running. Interrupting the Future and notifying result monitors happen after releasing that lock.
    private Future<Object> stopResultsThreadState() {
        resultsThreadStopped = true;
        resultsThreadGeneration.incrementAndGet();
        running.set(false);
        Future<Object> resultsFuture = future;
        future = null;
        running.notifyAll();
        return resultsFuture;
    }

    private void cancelResultsFuture(Future<Object> resultsFuture) {
        if (resultsFuture != null) {
            // Interrupt pending iterator work. The generation prevents late results from
            // this producer from entering a later execution of the query.
            resultsFuture.cancel(true);
        }
        signalResultsThreadWaiters();
    }

    /**
     * This is a method exposed for the QueryExpirationBean when it detects that we may be stuck in the next() call with currentPageCount &gt; 0. It attempts to
     * force the page to be returned by triggering any of the monitors that we may be waiting on and setting the forced return flag.
     */
    public void attemptForcedPageReturn() {
        // if we are still in an active call, then try to force a return
        if (hasActiveCall()) {
            synchronized (forcedReturn) {
                // if we have a current page count, then continue with the forced page return
                // otherwise we are already out of the results loop and are returning
                if (currentPageCount > 0) {
                    forcedReturn.set(true);
                    synchronized (hasNext) {
                        hasNext.notifyAll();
                    }
                    synchronized (gotNext) {
                        gotNext.notifyAll();
                    }
                }
            }
        }
    }

    private boolean isWorkLimitReached() {
        long maxWork = this.logic.getMaxWork();
        if (maxWork >= 0 && (this.getMetric().getNextCount() + this.getMetric().getSeekCount()) >= maxWork) {
            log.info("Query logic max work has been reached, aborting query.next call");
            this.getMetric().setLifecycle(QueryMetric.Lifecycle.MAXWORK);
            return true;
        }
        return false;
    }

    /**
     * Get the next results page
     *
     * @return a results page.
     * @throws Exception
     *             if there are issues
     */
    public ResultsPage next() throws Exception {
        // update AbstractRunningQuery.lastUsed
        touch();
        long pageStartTime = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("Starting next call at " + pageStartTime);
        }
        this.currentThread = Thread.currentThread();
        this.logic.setPageProcessingStartTime(pageStartTime);
        List<Object> resultList = new ArrayList<>();
        boolean hitIntermediateResult = false;
        boolean hitShortCircuitForLongRunningQuery = false;
        boolean hitForcedPageReturn = false;
        boolean hitResultsWaitTimeout = false;

        synchronized (forcedReturn) {
            currentPageCount = 0;
            forcedReturn.set(false);
        }

        long currentPageBytes = 0;
        int maxPageSize = Math.min(this.settings.getPagesize(), this.logic.getMaxPageSize());
        boolean asynchronous = useResultsThread && executor != null;

        int returnPageCount = 0;

        try {
            addNDC();
            synchronized (running) {
                pageInProgress = true;
                running.notifyAll();
            }

            // test for any exceptions prior to loop as hasNext() would likely be false;
            testForUncaughtException(resultList.size());

            try {
                // Synchronous iterators retain their existing look-ahead behavior. Async pages
                // check their limits before requesting work that might need an executor worker.
                while (!this.finished && (asynchronous || hasNext(pageStartTime))) {
                    // if we are canceled, then break out
                    if (this.canceled) {
                        log.info("Query has been cancelled, aborting query.next call");
                        this.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
                        break;
                    }
                    // if the number of results has reached our page size, then break out
                    if (currentPageCount >= this.settings.getPagesize()) {
                        log.info("Query requested page size had been reached, aborting query.next call");
                        break;
                    }
                    // if the logic had a max page size, and we have reached that, then break out
                    if (this.logic.getMaxPageSize() > 0 && currentPageCount >= this.logic.getMaxPageSize()) {
                        log.info("Query logic max page size has been reached, aborting query.next call");
                        break;
                    }
                    // if the logic had a page byte trigger, and we have reached that, then break out
                    if (this.logic.getPageByteTrigger() > 0 && currentPageBytes >= this.logic.getPageByteTrigger()) {
                        log.info("Query logic max page byte trigger has been reached, aborting query.next call");
                        break;
                    }
                    // if the logic had a max num results (across all pages) and we have reached that (or the maxResultsOverride if set), then break out
                    if (this.settings.isMaxResultsOverridden()) {
                        if (this.settings.getMaxResultsOverride() >= 0 && numResults >= this.settings.getMaxResultsOverride()) {
                            log.info("Max results override has been reached, aborting query.next call");
                            this.getMetric().setLifecycle(QueryMetric.Lifecycle.MAXRESULTS);
                            break;
                        }
                    } else if (this.maxResults >= 0 && numResults >= this.maxResults) {
                        log.info("Query logic max results has been reached, aborting query.next call");
                        this.getMetric().setLifecycle(QueryMetric.Lifecycle.MAXRESULTS);
                        break;
                    }
                    if (isWorkLimitReached()) {
                        break;
                    }
                    // A complete page needs no further iterator work or executor capacity.
                    if (asynchronous) {
                        if (!hasNext(pageStartTime)) {
                            break;
                        }
                        // The producer may have exhausted maxWork while this request waited for its next row.
                        if (isWorkLimitReached()) {
                            break;
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("hasNext(" + pageStartTime + ") returned");
                    }
                    // if we are the specified amount on the way to timing out on this call and we have results,
                    // determine whether we are on track to having enough results
                    // use the pagestart time for the time in call since we only care about the execution time of
                    // this page.
                    long pageTimeInCall = (System.currentTimeMillis() - pageStartTime);

                    if (timing != null && currentPageCount > 0 && timing.shouldReturnPartialResults(currentPageCount, maxPageSize, pageTimeInCall)) {
                        log.info("Query logic max expire before page is full, returning existing results " + currentPageCount + " " + maxPageSize + " "
                                        + pageTimeInCall + " " + timing);
                        break;
                    }

                    // if the forced return has been triggered, then return this page immediately
                    if (forcedReturn.get()) {
                        log.info("Query forced return detected with " + currentPageCount + " results");
                        break;
                    }

                    // now get the next object
                    Object o = getNext(pageStartTime);
                    if (log.isDebugEnabled()) {
                        log.debug("getNext(" + pageStartTime + ") returned with " + (o == null ? "null" : "result"));
                    }

                    // now that we got the next object, acknowledge via the counters
                    gotNext.decrementAndGet();
                    hasNext.decrementAndGet();

                    if (o instanceof EventBase && ((EventBase) o).isIntermediateResult()) {
                        log.info("Received an intermediate result");
                        // in this case we have timed out up stream somewhere, so lets return what we have
                        hitIntermediateResult = true;
                        break;
                    }

                    if (null == o) {
                        log.debug("Null result encountered, no more results");
                        this.finished = true;
                        terminateResultsThread();
                        break;
                    }

                    resultList.add(o);
                    if (this.logic.getPageByteTrigger() > 0) {
                        if (log.isDebugEnabled()) {
                            log.debug("Determining size of object");
                        }
                        currentPageBytes += ObjectSizeOf.Sizer.getObjectSize(o);
                        if (log.isDebugEnabled()) {
                            log.debug("Current page is up to " + currentPageBytes + " bytes");
                        }
                    }
                    currentPageCount++;
                    numResults++;

                    testForUncaughtException(resultList.size());
                }
            } catch (TimeoutException te) {
                hitResultsWaitTimeout = true;
                log.info("Hit the timeout waiting for a result");
                // This means the iter.hasNext() call didn't return within the allotted time. If this is a long running query,
                // then we want to signal that the caller should call next to keep going (as opposed to just returning the
                // page as COMPLETE)
                if (allowIntermediateEmptyPages) {
                    log.info("Short circuiting the long running query");
                    hitShortCircuitForLongRunningQuery = true;
                } else if (resultList.isEmpty()) {
                    log.warn("Query timed out waiting for next result");
                    terminateResultsThread();
                    throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT, "Query timed out waiting for next result");
                }
            }
            // if the last hasNext() call failed, then we would catch the exception here
            testForUncaughtException(resultList.size());

            // Update the metric
            long now = System.currentTimeMillis();
            this.getMetric().addPageTime(currentPageCount, now - pageStartTime, pageStartTime, now);
            // check the query plan for updates
            if (logic instanceof BaseQueryLogic) {
                BaseQueryLogic baseLogic = (BaseQueryLogic) logic;
                if (baseLogic.getConfig() != null && !StringUtils.isEmpty(baseLogic.getConfig().getQueryString())) {
                    this.getMetric().setPlan(baseLogic.getConfig().getQueryString());
                }
            }
            this.lastPageNumber.incrementAndGet();
            if (!resultList.isEmpty()) {
                this.getMetric().setLifecycle(QueryMetric.Lifecycle.RESULTS);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            this.getMetric().setError(e);
            terminateResultsThread();
            throw e;
        } finally {
            synchronized (running) {
                pageInProgress = false;
                running.notifyAll();
            }
            synchronized (forcedReturn) {
                returnPageCount = currentPageCount;
                hitForcedPageReturn = forcedReturn.get();
                currentPageCount = 0;
                forcedReturn.set(false);
            }

            // update AbstractRunningQuery.lastUsed in case this operation took a long time
            touch();
            removeNDC();

            if (this.queryMetrics != null) {
                try {
                    this.queryMetrics.updateMetric(this.getMetric());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        if (!resultList.isEmpty()) {
            log.info("Returning page of results");
            // we have results!

            // we also indicate whether we returned less than the requested page size in the response
            ResultsPage resultsPage = new ResultsPage(resultList,
                            ((hasNext.get() > 0 && numResults < this.maxResults && returnPageCount < maxPageSize) || hitIntermediateResult
                                            || hitShortCircuitForLongRunningQuery || hitForcedPageReturn || hitResultsWaitTimeout) ? ResultsPage.Status.PARTIAL
                                                            : ResultsPage.Status.COMPLETE);

            return resultsPage;
        } else {
            // we have no results. Let us determine whether we are done or not.

            // if we have hit an intermediate result or a short circuit then check to see how many times we hit this
            if (hitIntermediateResult || hitShortCircuitForLongRunningQuery) {
                currentTimeoutcount++;
                if (timing != null && currentTimeoutcount == timing.getMaxLongRunningTimeoutRetries()) {
                    log.warn("Query timed out waiting for results for too many ( " + currentTimeoutcount + ") cycles.");
                    terminateResultsThread();
                    // this means that we have timed out waiting for a result too many times over the course of this query.
                    // In this case we need to fail the next call with a timeout
                    throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT,
                                    "Query timed out waiting for results for too many (" + currentTimeoutcount + ") cycles.");
                } else {
                    log.info("Returning an empty partial results page");
                    // We are returning an empty page with a PARTIAL status to allow the query to continue running
                    return new ResultsPage(new ArrayList<>(), ResultsPage.Status.PARTIAL);
                }
            } else {
                log.info("Returning final empty page");
                terminateResultsThread();
                // This query is done, we have no more results to return.
                return new ResultsPage(Collections.emptyList(), ResultsPage.Status.NONE);
            }
        }
    }

    public void cancel() {
        this.canceled = true;

        terminateResultsThread();

        // change status to cancelled
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public AccumuloClient getClient() {
        return client;
    }

    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return connectionPriority;
    }

    public QueryLogic<?> getLogic() {
        return logic;
    }

    public Query getSettings() {
        return settings;
    }

    public TransformIterator getTransformIterator() {
        return iter;
    }

    protected Set<Authorizations> getCalculatedAuths() {
        return calculatedAuths;
    }

    protected QueryPredictor getPredictor() {
        return this.predictor;
    }

    public void setPredictor(QueryPredictor predictor) {
        this.predictor = predictor;
    }

    protected void applyPrediction(String context) {
        if (getPredictor() != null) {
            try {
                Set<Prediction> predictions = getPredictor().predict(this.getMetric());
                if (predictions != null) {
                    // now we have a predictions, lets broadcast
                    log.info("Model predictions: " + predictions);
                    context = (context == null ? "" : context + ' ');
                    for (Prediction prediction : predictions) {
                        // append the context to the predictions
                        this.getMetric().addPrediction(new Prediction(context + prediction.getName(), prediction.getPrediction()));
                    }
                }
            } catch (Exception e) {
                log.warn("Unable to apply query prediction", e);
            }
        }
    }

    public void closeConnection(AccumuloConnectionFactory factory) throws Exception {
        this.getMetric().setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);

        // terminate the results thread if any
        terminateResultsThread();

        if (iter != null && iter.getTransformer() instanceof WritesResultCardinalities) {
            ((WritesResultCardinalities) iter.getTransformer()).writeResultCardinalities();
        }

        if (client != null) {
            try {
                factory.returnClient(client);
                client = null;
            } finally {
                // only push metrics if this RunningQuery was initialized
                if (this.queryMetrics != null) {
                    try {
                        queryMetrics.updateMetric(this.getMetric());
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
            }
        }

        closeLogic();
    }

    private void closeLogic() {
        if (logic != null) {
            try {
                addNDC();
                logic.close();
            } catch (Exception e) {
                log.error("Exception occurred while closing query logic; may be innocuous if scanners were running.", e);
            } finally {
                removeNDC();
            }
        }
    }

    @Override
    public long getLastPageNumber() {
        return this.lastPageNumber.get();
    }

    @Override
    public String toString() {

        String host = System.getProperty("jboss.host.name");

        return new StringBuilder().append("host:").append(host).append(", id:").append(this.getSettings().getId()).append(", query:")
                        .append(StringEscapeUtils.escapeHtml(this.getSettings().getQuery())).append(", auths:")
                        .append(this.getSettings().getQueryAuthorizations()).append(", user:").append(this.getSettings().getOwner()).append(", queryLogic:")
                        .append(this.getSettings().getQueryLogicName()).append(", name:").append(this.getSettings().getQueryName()).append(", pagesize:")
                        .append(this.getSettings().getPagesize()).append(", begin:").append(this.getSettings().getBeginDate()).append(", end:")
                        .append(this.getSettings().getEndDate()).append(", expiration:").append(this.getSettings().getExpirationDate()).append(", params: ")
                        .append(this.getSettings().getParameters()).append(", callTime: ")
                        .append((this.getTimeOfCurrentCall() == 0) ? 0 : System.currentTimeMillis() - this.getTimeOfCurrentCall()).toString();

    }

    public QueryMetricsBean getQueryMetrics() {
        return queryMetrics;
    }

    public void setQueryMetrics(QueryMetricsBean queryMetrics) {
        if (logic != null && logic.getCollectQueryMetrics() == true) {
            this.queryMetrics = queryMetrics;
        }
    }

    /**
     * An interface used to force returning from a next call within a running query.
     */
    public interface RunningQueryTiming {
        boolean shouldReturnPartialResults(int pageSize, int maxPageSize, long timeInCall);

        int getMaxLongRunningTimeoutRetries();

        long getPageShortCircuitTimeoutMs();
    }

    /**
     * A noop implementation of the running query timing interface. -- only used by upstream tests
     */
    public static class RunningQueryTimingNoOp implements RunningQueryTiming {
        public boolean shouldReturnPartialResults(int pageSize, int maxPageSize, long timeInCall) {
            return false;
        }

        public int getMaxLongRunningTimeoutRetries() {
            return 0;
        }

        // hardcoded because only used by upstream tests.
        public long getPageShortCircuitTimeoutMs() {
            return 300000000000000L;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    private void testForUncaughtException(int numResults) throws QueryException {
        QueryUncaughtExceptionHandler handler = settings.getUncaughtExceptionHandler();
        if (handler != null) {
            ImmutablePair<Throwable,Thread> uncaughtException = handler.getUncaughtException();
            if (uncaughtException != null) {
                if (numResults > 0) {
                    log.warn("Exception with Partial Results: resultList.getResults().size() is " + numResults + ", and there was an UncaughtException:"
                                    + uncaughtException.getLeft() + " in thread " + uncaughtException.getRight());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Throwing:" + uncaughtException.getLeft() + " for query with no results");
                    }
                }
                if (uncaughtException.getLeft() instanceof QueryException) {
                    throw ((QueryException) uncaughtException.getLeft());
                }
                throw new QueryException(uncaughtException.getLeft());
            }
        }
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Thread getCurrentThread() {
        return currentThread;
    }

    public int getCurrentPageCount() {
        return currentPageCount;
    }

    public RunningQueryTiming getTiming() {
        return timing;
    }
}
