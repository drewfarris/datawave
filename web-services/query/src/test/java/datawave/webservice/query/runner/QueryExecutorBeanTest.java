package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ejb.EJBContext;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Status;
import javax.transaction.UserTransaction;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.log4j.Logger;
import org.jboss.resteasy.mock.MockDispatcherFactory;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.spi.Dispatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.Uninterruptibles;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.audit.PrivateAuditConstants;
import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.common.connection.AccumuloClientPoolFactory;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.predict.QueryPredictor;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.QueryPersistence;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.AuditService;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.audit.DefaultAuditParameterBuilder;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.query.cache.ClosedQueryCache;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean.Triple;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryExpirationBean;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.limit.QueryHeartbeat;
import datawave.webservice.query.limit.QueryHeartbeatCache;
import datawave.webservice.query.limit.QueryLimiter;
import datawave.webservice.query.limit.QueryLimiterResponse;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.MapUtils;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class QueryExecutorBeanTest {
    // Fields for building generic default queries
    private final String queryLogicName = "EventQueryLogic";
    private final String queryName = "Something";
    private final String query = "FOO == BAR";
    Date beginDate = new Date();
    Date endDate = beginDate;
    private final Date expirationDate = DateUtils.addDays(new Date(), 1);
    private final int pagesize = 10;
    private final QueryPersistence persist = QueryPersistence.TRANSIENT;

    // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
    // setup principal mock
    private final String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
    private final String[] auths = new String[] {"PRIVATE", "PUBLIC"};

    private static final Logger log = Logger.getLogger(QueryExecutorBeanTest.class);

    // QueryExecutorBean dependencies
    private QueryCache cache;
    private ClosedQueryCache closedCache;
    private AccumuloConnectionRequestBean connectionRequestBean;
    private AccumuloConnectionFactory connectionFactory;
    private AuditBean auditor;
    private AuditService auditService;
    private QueryMetricsBean metrics;
    private QueryLogicFactoryImpl queryLogicFactory;
    private QueryExpirationProperties queryExpirationConf;
    private Persister persister;
    private QueryPredictor predictor;
    private ResponseObjectFactory responseObjectFactory;
    private EJBContext ctx;
    private CreatedQueryLogicCacheBean qlCache;
    private QueryExecutorBean bean;
    private Dispatcher dispatcher;
    private MockHttpRequest request;
    private MockHttpResponse response;
    private QueryLimiter queryLimiter;

    @BeforeEach
    public void setup() throws Exception {
        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        QueryTraceCache traceCache = new QueryTraceCache();
        ReflectionTestUtils.invokeMethod(traceCache, "init");

        cache = new QueryCache();
        cache.init();

        closedCache = new ClosedQueryCache();

        bean = new QueryExecutorBean();

        connectionFactory = mock(AccumuloConnectionFactory.class);
        auditor = new AuditBean();
        auditService = mock(AuditService.class);
        metrics = mock(QueryMetricsBean.class);
        queryLogicFactory = mock(QueryLogicFactoryImpl.class);
        persister = mock(Persister.class);
        predictor = mock(QueryPredictor.class);
        ctx = mock(EJBContext.class);
        qlCache = new CreatedQueryLogicCacheBean();
        queryExpirationConf = new QueryExpirationProperties();
        queryExpirationConf.setShortCircuitCheckTime(45);
        queryExpirationConf.setShortCircuitTimeout(58);
        queryExpirationConf.setCallTimeout(60);
        connectionRequestBean = mock(AccumuloConnectionRequestBean.class);
        responseObjectFactory = mock(ResponseObjectFactory.class);
        queryLimiter = mock(QueryLimiter.class);
        ReflectionTestUtils.setField(auditor, "auditService", auditService);
        ReflectionTestUtils.setField(auditor, "auditParameterBuilder", new DefaultAuditParameterBuilder());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", ctx);
        ReflectionTestUtils.setField(bean, "queryCache", cache);
        ReflectionTestUtils.setField(bean, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(bean, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(bean, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(bean, "auditor", auditor);
        ReflectionTestUtils.setField(bean, "metrics", metrics);
        ReflectionTestUtils.setField(bean, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(bean, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(bean, "persister", persister);
        ReflectionTestUtils.setField(bean, "predictor", predictor);
        ReflectionTestUtils.setField(bean, "ctx", ctx);
        ReflectionTestUtils.setField(bean, "qlCache", qlCache);
        ReflectionTestUtils.setField(bean, "queryTraceCache", traceCache);
        ReflectionTestUtils.setField(bean, "traceInfos", HashMultimap.create());
        ReflectionTestUtils.setField(bean, "lookupUUIDConfiguration", new LookupUUIDConfiguration());
        ReflectionTestUtils.setField(bean, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(bean, "qp", new DefaultQueryParameters());
        ReflectionTestUtils.setField(bean, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(bean, "accumuloConnectionRequestBean", connectionRequestBean);
        ReflectionTestUtils.setField(bean, "queryLimiter", queryLimiter);
        // RESTEasy mock stuff
        dispatcher = MockDispatcherFactory.createDispatcher();
        dispatcher.getRegistry().addSingletonResource(bean, "/DataWave/Query");
        response = new MockHttpResponse();
    }

    @Test
    public void testNextWhenLockedKeepsOtherRequestsLock() throws Exception {
        String id = UUID.randomUUID().toString();
        Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn("owner");
        when(ctx.getCallerPrincipal()).thenReturn(principal);
        when(ctx.getUserTransaction()).thenReturn(mock(UserTransaction.class));
        when(responseObjectFactory.getEventQueryResponse()).thenReturn(mock(EventQueryResponseBase.class));

        // simulate another request holding the lock
        assertTrue(cache.lock(id));

        assertThrows(DatawaveWebApplicationException.class, () -> bean.next(id));
        assertFalse(cache.lock(id), "next() released a lock it did not acquire");
    }

    @Test
    public void testResetWhenLockedKeepsOtherRequestsLock() throws Exception {
        String id = UUID.randomUUID().toString();
        Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn("owner");
        when(ctx.getCallerPrincipal()).thenReturn(principal);
        when(ctx.getUserTransaction()).thenReturn(mock(UserTransaction.class));

        Query settings = mock(Query.class);
        when(settings.getOwner()).thenReturn("owner");
        RunningQuery runningQuery = mock(RunningQuery.class);
        when(runningQuery.getSettings()).thenReturn(settings);
        cache.put(id, runningQuery);

        // simulate another request holding the lock
        assertTrue(cache.lock(id));

        assertThrows(DatawaveWebApplicationException.class, () -> bean.reset(id));
        assertFalse(cache.lock(id), "reset() released a lock it did not acquire");
    }

    @Test
    public void testTriple_Nulls() throws Exception {
        Triple subject = new Triple(null, null, null);
        subject.hashCode();
        assertNotEquals(subject, null, "Should not be equal");
        assertNotEquals(subject, new Triple("test", null, null), "Should not be equal");
    }

    @ParameterizedTest(name = "{0}, finished={1}, limiterEnabled={2}: releases={3}")
    @CsvSource({"COMPLETE, false, true, 0", "PARTIAL, false, true, 0", "COMPLETE, true, true, 1", "PARTIAL, true, true, 1", "NONE, false, true, 1",
            "COMPLETE, false, false, 0", "COMPLETE, true, false, 0", "NONE, false, false, 0"})
    @SuppressWarnings("unchecked")
    public void testPageCompletionAccounting(ResultsPage.Status status, boolean finished, boolean limiterEnabled, int expectedReleases) throws Exception {
        QueryImpl settings = createNewQuery();
        settings.setPagesize(status == ResultsPage.Status.PARTIAL ? 2 : 1);
        String queryId = settings.getId().toString();
        RunningQuery runningQuery = mock(RunningQuery.class);
        QueryLogic<Object> logic = mock(QueryLogic.class);
        QueryLogicTransformer<Object,Object> transformer = mock(QueryLogicTransformer.class);
        ResultsPage page = new ResultsPage(status == ResultsPage.Status.NONE ? Collections.emptyList() : Collections.singletonList("row"), status);
        DefaultEventQueryResponse pageResponse = new DefaultEventQueryResponse();
        when(runningQuery.next()).thenReturn(page);
        when(runningQuery.isFinished()).thenReturn(finished);
        doReturn(logic).when(runningQuery).getLogic();
        when(runningQuery.getSettings()).thenReturn(settings);
        when(runningQuery.getMetric()).thenReturn(new QueryMetric());
        when(logic.isQueryLimiterEnabled()).thenReturn(limiterEnabled);
        when(logic.getEnrichedTransformer(settings)).thenReturn(transformer);
        when(transformer.createResponse(page)).thenReturn(pageResponse);

        if (status == ResultsPage.Status.NONE) {
            assertThrows(NoResultsException.class, () -> ReflectionTestUtils.invokeMethod(bean, "_next", runningQuery, queryId, Collections.emptyList()));
        } else {
            assertSame(pageResponse, ReflectionTestUtils.invokeMethod(bean, "_next", runningQuery, queryId, Collections.emptyList()));
        }
        verify(queryLimiter, times(expectedReleases)).stopCountingQueryTowardsLimits(queryId);
    }

    @ParameterizedTest(name = "{0}: accounting releases={1}")
    @CsvSource({"limit, 0", "limiterError, 0", "closeError, 1"})
    @SuppressWarnings("unchecked")
    public void testResetFailureAccounting(String failure, int expectedReleases) throws Exception {
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        String queryId = settings.getId().toString();
        QueryLogic<Object> logic = mock(QueryLogic.class);
        when(logic.isQueryLimiterEnabled()).thenReturn(true);
        AccumuloClient client = mock(AccumuloClient.class);
        TransformIterator<String,String> iterator = new TransformIterator<>(Collections.singletonList("row").iterator(), value -> value);
        when(logic.initialize(eq(client), eq(settings), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getTransformIterator(settings)).thenReturn(iterator);
        RunningQuery runningQuery = new RunningQuery(client, AccumuloConnectionFactory.Priority.NORMAL, logic, settings, null, null,
                        new QueryMetricFactoryImpl());
        BaseQueryMetric.Lifecycle originalLifecycle = runningQuery.getMetric().getLifecycle();
        cache.put(queryId, runningQuery);

        QueryHeartbeat heartbeat = new QueryHeartbeat(queryId, Collections.emptyList());
        QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
        heartbeats.put(heartbeat);
        queryLimiter = spy(new QueryLimiter());
        queryLimiter.setHeartbeatCache(heartbeats);
        ReflectionTestUtils.setField(bean, "queryLimiter", queryLimiter);
        doReturn("limit".equals(failure) ? QueryLimiterResponse.metLimit("at limit") : QueryLimiterResponse.hasNotMetLimit()).when(queryLimiter)
                        .checkForLimits(userDN, null, queryLogicName);
        if ("limiterError".equals(failure)) {
            doThrow(new IOException("limit check failed")).when(queryLimiter).checkForLimits(userDN, null, queryLogicName);
        } else if ("closeError".equals(failure)) {
            doThrow(new IOException("client return failed")).when(connectionFactory).returnClient(client);
        }
        UserTransaction transaction = mock(UserTransaction.class);
        when(transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(ctx.getUserTransaction()).thenReturn(transaction);
        when(ctx.getCallerPrincipal()).thenReturn(() -> "owner");

        assertThrows(DatawaveWebApplicationException.class, () -> bean.reset(queryId));

        assertSame(runningQuery, cache.get(queryId));
        assertSame(client, runningQuery.getClient());
        assertSame(iterator, runningQuery.getTransformIterator());
        assertFalse(runningQuery.isFinished());
        assertFalse(runningQuery.isCanceled());
        assertEquals(expectedReleases == 0 ? originalLifecycle : BaseQueryMetric.Lifecycle.CLOSED, runningQuery.getMetric().getLifecycle());
        assertSame(expectedReleases == 0 ? heartbeat : null, heartbeats.get(queryId));
        verify(queryLimiter).checkForLimits(userDN, null, queryLogicName);
        verify(queryLimiter, times(expectedReleases)).stopCountingQueryTowardsLimits(queryId);
        verify(connectionFactory, times(expectedReleases)).returnClient(client);
        verify(logic).initialize(eq(client), eq(settings), any());
        verify(transaction).commit();
    }

    /**
     * Close returned an initializer's installed client to the pool. If initialization subsequently threw, reset's error path returned its local client again.
     * After another query borrowed that same client, the stale return released the other query's active lease and removed its tracking entry.
     *
     * Exercise reset/adminClose and real factory/pool methods with mocked clients. Hold initialization across close, reborrow the released client, then fail
     * after acquiring a late resource. The other lease must remain active and the late resource must be reclaimed. Close marks the execution terminal; cache
     * eviction may wait for the reset's outstanding cleanup to finish.
     *
     * @param failInSetup
     *            whether setupQuery() rather than initialize() fails after close
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testResetFailureAfterCloseDoesNotReturnAnotherQueriesLease(boolean failInSetup) throws Exception {
        AccumuloClient pooledClient = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(pooledClient);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery runningQuery = queryForReset(mock(AccumuloClient.class), settings, logic);
        runningQuery.closeConnection(factory);
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, runningQuery);
        CountDownLatch initializing = new CountDownLatch(1);
        CountDownLatch releaseInitializer = new CountDownLatch(1);
        AtomicBoolean resourcesOpen = new AtomicBoolean();
        doAnswer(invocation -> {
            resourcesOpen.set(false);
            return null;
        }).when(logic).close();
        if (failInSetup) {
            doAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseInitializer, 5, TimeUnit.SECONDS), "Setup was not released");
                resourcesOpen.set(true);
                throw new IllegalStateException("Setup failed after close");
            }).when(logic).setupQuery(any());
        } else {
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseInitializer, 5, TimeUnit.SECONDS), "Initializer was not released");
                resourcesOpen.set(true);
                throw new IllegalStateException("Initialization failed after close");
            });
        }
        ExecutorService requests = Executors.newSingleThreadExecutor();
        AccumuloClient otherOwner = null;
        Future<?> reset = requests.submit(() -> bean.reset(id));
        try {
            assertTrue(initializing.await(2, TimeUnit.SECONDS), "Reset did not enter initialization");
            assertSame(pooledClient, runningQuery.getClient());
            bean.adminClose(id);
            assertTrue(runningQuery.isMarkedForRemoval(), "Close must terminate the execution while reset still owns cleanup");
            assertNull(runningQuery.getClient());
            assertEquals(0, pool.getNumActive(), "Close did not return its installed client");
            otherOwner = pool.borrowObject(new HashMap<>());
            assertSame(pooledClient, otherOwner, "The regression requires reuse of the returned client");
            releaseInitializer.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
            assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
            assertEquals(1, pool.getNumActive(), "Reset returned a client already reborrowed by another query");
            assertTrue(pool.connectorCameFromHere(otherOwner), "Reset removed the other query's lease tracking");
            assertFalse(resourcesOpen.get(), "The exceptional initializer leaked its late resources");
            assertNull(cache.get(id), "Completed reset cleanup must release its cache owner");
        } finally {
            releaseInitializer.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            if (otherOwner != null && pool.connectorCameFromHere(otherOwner)) {
                pool.returnObject(otherOwner);
            }
            logic.close();
            pool.close();
        }
    }

    /**
     * A client rejected before installation still belongs to reset's caller. Close during prediction removes the query but cannot return that uninstalled
     * client. Failure cleanup must return this unused lease, even though an earlier execution used and returned the identical pooled client object.
     */
    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testResetRejectedBeforeClientInstallationReturnsUnusedLease() throws Exception {
        AccumuloClient pooledClient = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(pooledClient);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery runningQuery = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
        runningQuery.closeConnection(factory);
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, runningQuery);
        CountDownLatch predicting = new CountDownLatch(1);
        CountDownLatch releasePredictor = new CountDownLatch(1);
        QueryPredictor<BaseQueryMetric> predictor = metric -> {
            predicting.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releasePredictor, 5, TimeUnit.SECONDS), "Predictor was not released");
            return Collections.emptySet();
        };
        runningQuery.setPredictor(predictor);
        ExecutorService requests = Executors.newSingleThreadExecutor();
        Future<?> reset = requests.submit(() -> bean.reset(id));
        try {
            assertTrue(predicting.await(2, TimeUnit.SECONDS), "Reset did not enter prediction");
            assertNull(runningQuery.getClient(), "Prediction must precede client installation");
            bean.adminClose(id);
            assertEquals(1, pool.getNumActive(), "Close returned a client it had not accepted");
            releasePredictor.countDown();
            assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
            assertEquals(0, pool.getNumActive(), "Failure cleanup did not return the caller's unused client");
            assertEquals(1, pool.getNumIdle());
            verify(logic).initialize(eq(pooledClient), eq(settings), any());
        } finally {
            releasePredictor.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            pool.close();
        }
    }

    /**
     * Accounting can fail after borrow but before setClient() is invoked. That freshly borrowed lease is caller-owned and must be returned independently of any
     * previous initialization record for the same pooled client object.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testResetFailureBeforeInitializationReturnsUnusedLease() throws Exception {
        AccumuloClient pooledClient = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(pooledClient)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            QueryLogic<Object> logic = mock(QueryLogic.class);
            RunningQuery runningQuery = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
            runningQuery.closeConnection(factory);
            configureResetOwner(factory);
            cache.put(settings.getId().toString(), runningQuery);
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            when(queryLimiter.checkForLimits(any(), any(), any())).thenReturn(QueryLimiterResponse.hasNotMetLimit());
            doThrow(new IOException("Accounting failed after borrow")).when(queryLimiter).countQueryTowardsLimits(any(), any(), any(), any());

            assertThrows(DatawaveWebApplicationException.class, () -> bean.reset(settings.getId().toString()));
            assertEquals(0, pool.getNumActive(), "A failure before initialization leaked the unused lease");
            assertEquals(1, pool.getNumIdle());
            verify(logic).initialize(eq(pooledClient), eq(settings), any());
        }
    }

    /**
     * Close detached and claimed its client before returning it, but recovered a failed return only if the producer generation was unchanged. Cancel/terminate
     * during that return changed the generation without transferring the lease, so the query lost its client while the pool still tracked an active loan.
     *
     * Hold a real pooled loan across a return that fails before releasing it. The query must retain that same client for a later close to return successfully.
     * The no-termination case controls the ordinary failed-return behavior, and the original return exception must be preserved in every case.
     *
     * @param termination
     *            the producer termination that overlaps the failed return, or none for the control
     */
    @ParameterizedTest
    @ValueSource(strings = {"none", "cancel", "terminate"})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testFailedCloseRetainsLeaseAfterProducerTermination(String termination) throws Exception {
        AccumuloClient pooledClient = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(pooledClient);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        RunningQuery runningQuery = queryForReset(pool.borrowObject(new HashMap<>()), createNewQuery(), mock(QueryLogic.class));
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch releaseReturn = new CountDownLatch(1);
        AtomicBoolean firstReturn = new AtomicBoolean(true);
        IOException originalFailure = new IOException("Return failed before releasing its lease");
        doAnswer(invocation -> {
            if (firstReturn.compareAndSet(true, false)) {
                returning.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Pool return was not released");
                throw originalFailure;
            }
            return invocation.callRealMethod();
        }).when(factory).returnClient(pooledClient);
        ExecutorService requests = Executors.newSingleThreadExecutor();
        Future<?> close = requests.submit(() -> {
            runningQuery.closeConnection(factory);
            return null;
        });
        try {
            assertTrue(returning.await(2, TimeUnit.SECONDS), "Close did not reach its pool return");
            assertEquals(1, pool.getNumActive(), "The original lease must still be active before return");
            if ("cancel".equals(termination)) {
                runningQuery.cancel();
            } else if ("terminate".equals(termination)) {
                runningQuery.terminateResultsThread();
            }
            releaseReturn.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> close.get(2, TimeUnit.SECONDS));
            assertSame(originalFailure, failure.getCause(), "Recovery replaced the original pool-return error");
            assertSame(pooledClient, runningQuery.getClient(), "Termination lost ownership of the failed-return client");
            assertTrue(pool.connectorCameFromHere(pooledClient), "The failed return must leave the loan tracked");

            runningQuery.closeConnection(factory);
            assertNull(runningQuery.getClient());
            assertEquals(0, pool.getNumActive(), "A later close could not release the retained loan");
            assertEquals(1, pool.getNumIdle());
            assertFalse(pool.connectorCameFromHere(pooledClient));
        } finally {
            releaseReturn.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            if (pool.connectorCameFromHere(pooledClient)) {
                pool.returnObject(pooledClient);
            }
            runningQuery.getLogic().close();
            pool.close();
        }
    }

    /**
     * A pending reset initializer owned a pooled client when adminClose detached it for return. Overlapping adminCancel changed the generation and removed the
     * query; when the first return failed, close kept its ownership claim set. Reset's later initialization failure therefore skipped the still-active loan.
     *
     * Gate initialize and the first return independently, then call the real reset/adminClose/adminCancel methods with a real pool. Reset's failure cleanup
     * must release the loan even though removal has begun. Keep the cached cleanup owner until that loan is returned; without adminCancel, the control
     * exercises the same failures and normal recovery.
     *
     * @param cancelDuringReturn
     *            whether adminCancel overlaps adminClose's pending return
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testResetFailureReleasesLeaseAfterOverlappingFailedClose(boolean cancelDuringReturn) throws Exception {
        AccumuloClient pooledClient = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(pooledClient);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery runningQuery = queryForReset(mock(AccumuloClient.class), settings, logic);
        runningQuery.closeConnection(factory);
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, runningQuery);
        CountDownLatch initializing = new CountDownLatch(1);
        CountDownLatch releaseInitializer = new CountDownLatch(1);
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch releaseReturn = new CountDownLatch(1);
        AtomicBoolean firstReturn = new AtomicBoolean(true);
        when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
            initializing.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releaseInitializer, 5, TimeUnit.SECONDS), "Initializer was not released");
            throw new IllegalStateException("Initialization failed after the failed close");
        });
        doAnswer(invocation -> {
            if (firstReturn.compareAndSet(true, false)) {
                returning.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Pool return was not released");
                throw new IOException("Return failed before releasing its lease");
            }
            return invocation.callRealMethod();
        }).when(factory).returnClient(pooledClient);
        ExecutorService requests = Executors.newFixedThreadPool(2);
        Future<?> reset = requests.submit(() -> bean.reset(id));
        try {
            assertTrue(initializing.await(2, TimeUnit.SECONDS), "Reset did not enter initialization");
            assertSame(pooledClient, runningQuery.getClient());
            Future<?> close = requests.submit(() -> bean.adminClose(id));
            assertTrue(returning.await(2, TimeUnit.SECONDS), "Admin close did not reach its pool return");
            if (cancelDuringReturn) {
                bean.adminCancel(id);
            }
            releaseReturn.countDown();
            close.get(2, TimeUnit.SECONDS);
            assertSame(runningQuery, cache.get(id), "Removal discarded the failed-return cleanup owner");
            assertEquals(1, pool.getNumActive(), "The gated return failed before releasing its loan");

            releaseInitializer.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
            assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
            assertNull(runningQuery.getClient());
            assertEquals(0, pool.getNumActive(), "Reset failure skipped a loan still claimed by the failed close");
            assertEquals(1, pool.getNumIdle());
            assertFalse(pool.connectorCameFromHere(pooledClient), "Failure cleanup left an orphaned tracking entry");
            assertNull(cache.get(id), "Completed removal retained its cleanup owner");
        } finally {
            releaseReturn.countDown();
            releaseInitializer.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            if (pool.connectorCameFromHere(pooledClient)) {
                pool.returnObject(pooledClient);
            }
            logic.close();
            pool.close();
        }
    }

    /**
     * Administrative close detached client A while its return was pending. Reset treated the empty slot as permission to borrow B and initialize the same
     * query. A successful old close then returned only A and removed the query, orphaning B; if both operations failed, the older A loan was orphaned instead.
     *
     * Hold close's first return with two real pool clients available. Attempt reset before releasing close. Removal must reject the replacement before
     * initialization, retain a cleanup owner after a failed return, and leave no active loans after close succeeds or is retried.
     *
     * @param failReturn
     *            whether the old client's first return fails
     * @param failInitialization
     *            whether an incorrectly admitted replacement initializer fails
     */
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testResetDuringAdministrativeCloseDoesNotOrphanEitherLoan(boolean failReturn, boolean failInitialization) throws Exception {
        AccumuloClient oldClient = mock(AccumuloClient.class);
        AccumuloClient replacement = mock(AccumuloClient.class);
        AccumuloClientPool pool = poolWithClients(oldClient, replacement);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, query);
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch releaseReturn = new CountDownLatch(1);
        AtomicBoolean firstReturn = new AtomicBoolean(true);
        AtomicBoolean resourcesOpen = new AtomicBoolean(true);
        AtomicInteger replacementInitializations = new AtomicInteger();
        doAnswer(invocation -> {
            resourcesOpen.set(false);
            return null;
        }).when(logic).close();
        when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
            replacementInitializations.incrementAndGet();
            resourcesOpen.set(true);
            if (failInitialization) {
                throw new IllegalStateException("Replacement initialization failed");
            }
            return mock(GenericQueryConfiguration.class);
        });
        doAnswer(invocation -> {
            if (firstReturn.compareAndSet(true, false)) {
                returning.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Old return was not released");
                if (failReturn) {
                    throw new IOException("Old return failed before releasing its loan");
                }
            }
            return invocation.callRealMethod();
        }).when(factory).returnClient(oldClient);
        ExecutorService requests = Executors.newFixedThreadPool(2);
        try {
            Future<?> close = requests.submit(() -> bean.adminClose(id));
            assertTrue(returning.await(2, TimeUnit.SECONDS), "Administrative close did not reach return");
            Future<?> reset = requests.submit(() -> bean.reset(id));
            boolean resetRejected = false;
            try {
                reset.get(2, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                resetRejected = true;
            }
            releaseReturn.countDown();
            close.get(2, TimeUnit.SECONDS);
            if (failReturn) {
                assertSame(query, cache.get(id), "Failed close removed the only owner of its loan");
                assertSame(oldClient, query.getClient(), "Failed close discarded the old loan");
                bean.adminClose(id);
            }
            assertEquals(0, pool.getNumActive(), "Close/reset left an orphaned pool loan");
            assertFalse(pool.connectorCameFromHere(oldClient));
            assertFalse(pool.connectorCameFromHere(replacement));
            assertNull(cache.get(id));
            assertNull(query.getClient());
            assertFalse(resourcesOpen.get());
            assertTrue(resetRejected, "Reset was admitted during administrative removal");
            assertEquals(0, replacementInitializations.get(), "Removal allowed replacement resources to be opened");
        } finally {
            releaseReturn.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            returnTrackedClients(pool, oldClient, replacement);
            logic.close();
            pool.close();
        }
    }

    /**
     * A guard covering only the return callback leaves another admission window: the connection has been returned and logic closed, but the API has not yet
     * evicted the cached query. Pause exactly there and reborrow the identical physical client through reset. The cached object must remain terminal for
     * initialization until removal finishes, rather than silently accepting a new loan that the older API subsequently removes.
     */
    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testResetCannotReopenQueryBetweenLogicCloseAndCacheEviction() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(client);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery query = spy(queryForReset(pool.borrowObject(new HashMap<>()), settings, logic));
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, query);
        CountDownLatch logicClosed = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        doAnswer(invocation -> {
            invocation.callRealMethod();
            logicClosed.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releaseClose, 5, TimeUnit.SECONDS), "API close was not released");
            return null;
        }).when(query).closeConnection(factory);
        ExecutorService requests = Executors.newFixedThreadPool(2);
        try {
            Future<?> close = requests.submit(() -> bean.adminClose(id));
            assertTrue(logicClosed.await(2, TimeUnit.SECONDS), "Close did not finish logic cleanup");
            assertSame(query, cache.get(id), "The reproduction requires the old object to remain cached");
            assertNull(query.getClient());
            Future<?> reset = requests.submit(() -> bean.reset(id));
            boolean rejected = false;
            try {
                reset.get(2, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                rejected = true;
            }
            releaseClose.countDown();
            close.get(2, TimeUnit.SECONDS);
            assertEquals(0, pool.getNumActive(), "Reset installed a loan after logic close and lost its cache owner");
            assertTrue(rejected, "The cached object accepted reset before eviction finished");
            verify(logic).initialize(eq(client), eq(settings), any());
            assertNull(cache.get(id));
        } finally {
            releaseClose.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            returnTrackedClients(pool, client);
            pool.close();
        }
    }

    /**
     * Reset can fetch a live query before removal and retain a newly borrowed, caller-owned loan afterward. Hold either getClient's return or accounting, then
     * administratively close before setClient is invoked. Reusing the identical physical client must not make its old returned lease suppress cleanup of this
     * new unused loan. Removal must also prevent the stale reset from opening resources. Verify eviction after cleanup, allowing the cache to retain the
     * admitted borrower's cleanup owner while reset is pending.
     *
     * @param duringAccounting
     *            whether to pause after borrow in accounting rather than before getClient returns
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testStaleResetReturnsReborrowedClientAfterAdministrativeRemoval(boolean duringAccounting) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(client);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, query);
        CountDownLatch borrowed = new CountDownLatch(1);
        CountDownLatch releaseReset = new CountDownLatch(1);
        if (duringAccounting) {
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            when(queryLimiter.checkForLimits(any(), any(), any())).thenReturn(QueryLimiterResponse.hasNotMetLimit());
            doAnswer(invocation -> {
                borrowed.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReset, 5, TimeUnit.SECONDS), "Accounting was not released");
                return null;
            }).when(queryLimiter).countQueryTowardsLimits(any(), any(), any(), any());
        } else {
            doAnswer(invocation -> {
                AccumuloClient loan = (AccumuloClient) invocation.callRealMethod();
                assertSame(client, loan, "Reset must reborrow the original client");
                borrowed.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReset, 5, TimeUnit.SECONDS), "Borrow was not released");
                return loan;
            }).when(factory).getClient(any(), any(), any(), any(), any());
        }
        ExecutorService requests = Executors.newSingleThreadExecutor();
        try {
            Future<?> reset = requests.submit(() -> bean.reset(id));
            assertTrue(borrowed.await(2, TimeUnit.SECONDS), "Reset did not hold its caller-owned loan");
            assertNull(query.getClient(), "The reborrowed client must not yet be installed");
            assertEquals(1, pool.getNumActive());
            bean.adminClose(id);
            assertTrue(query.isMarkedForRemoval(), "Close must terminate the execution while reset still owns cleanup");
            releaseReset.countDown();
            boolean rejected = false;
            try {
                reset.get(2, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                rejected = true;
            }
            assertEquals(0, pool.getNumActive(), "Stale reset orphaned its unused reborrowed client");
            assertTrue(rejected, "A reset fetched before removal was allowed to reopen afterward");
            verify(logic).initialize(eq(client), eq(settings), any());
            assertNull(query.getClient());
            assertNull(cache.get(id), "Completed unused-loan cleanup must release its cache owner");
        } finally {
            releaseReset.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            returnTrackedClients(pool, client);
            pool.close();
        }
    }

    /**
     * Reset's own close may already be returning its old loan when administrative removal begins. The installed slot is empty, but the first closer still owns
     * a pending return. Keep that cached cleanup owner until the return completes; if it fails, the original reset must finish removal by retrying its retained
     * loan. A successful return must not allow the stale reset to allocate another client after removal.
     *
     * @param failReturn
     *            whether reset's first return fails
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testAdministrativeCloseRetainsPendingResetReturn(boolean failReturn) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(client);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, query);
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch releaseReturn = new CountDownLatch(1);
        AtomicBoolean firstReturn = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (firstReturn.compareAndSet(true, false)) {
                returning.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Reset's old return was not released");
                if (failReturn) {
                    throw new IOException("Reset's old return failed");
                }
            }
            return invocation.callRealMethod();
        }).when(factory).returnClient(client);
        ExecutorService requests = Executors.newSingleThreadExecutor();
        try {
            Future<?> reset = requests.submit(() -> bean.reset(id));
            assertTrue(returning.await(2, TimeUnit.SECONDS), "Reset did not close its original loan");
            bean.adminClose(id);
            assertSame(query, cache.get(id), "Administrative removal forgot a return still owned by reset");
            releaseReturn.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
            assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
            assertEquals(0, pool.getNumActive(), "Reset did not finish returning its removal-owned loan");
            assertNull(cache.get(id));
            assertNull(query.getClient());
            verify(logic).initialize(eq(client), eq(settings), any());
        } finally {
            releaseReturn.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            returnTrackedClients(pool, client);
            pool.close();
        }
    }

    /**
     * A close retains its RunningQuery reference while cleanup is pending. Another object may subsequently occupy the same cache key. Eviction must compare
     * that cached instance, so an old close cannot remove a replacement object and orphan the replacement's separate client loan.
     */
    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testOlderAdministrativeCloseDoesNotEvictReplacementCacheEntry() throws Exception {
        AccumuloClient oldClient = mock(AccumuloClient.class);
        AccumuloClient newClient = mock(AccumuloClient.class);
        AccumuloClientPool pool = poolWithClients(oldClient, newClient);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        RunningQuery oldQuery = queryForReset(pool.borrowObject(new HashMap<>()), settings, mock(QueryLogic.class));
        configureResetOwner(factory);
        String id = settings.getId().toString();
        cache.put(id, oldQuery);
        CountDownLatch returning = new CountDownLatch(1);
        CountDownLatch releaseReturn = new CountDownLatch(1);
        doAnswer(invocation -> {
            returning.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Old return was not released");
            return invocation.callRealMethod();
        }).when(factory).returnClient(oldClient);
        ExecutorService requests = Executors.newSingleThreadExecutor();
        try {
            Future<?> close = requests.submit(() -> bean.adminClose(id));
            assertTrue(returning.await(2, TimeUnit.SECONDS));
            RunningQuery replacement = queryForReset(pool.borrowObject(new HashMap<>()), settings, mock(QueryLogic.class));
            cache.put(id, replacement);
            releaseReturn.countDown();
            close.get(2, TimeUnit.SECONDS);
            assertSame(replacement, cache.get(id), "Old close evicted another RunningQuery instance");
            assertEquals(1, pool.getNumActive(), "The replacement must still own its cached loan");
            bean.adminClose(id);
            assertEquals(0, pool.getNumActive());
            assertNull(cache.get(id));
        } finally {
            releaseReturn.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            returnTrackedClients(pool, oldClient, newClient);
            pool.close();
        }
    }

    /**
     * Expiration removes cached queries through a second cleanup path. A failed return must retain the same terminal cleanup owner for a later expiration pass
     * to retry, rather than deleting the only reference while the real pool still tracks an active client.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testExpirationRetainsFailedReturnForRetry() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            QueryImpl settings = createNewQuery();
            RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, mock(QueryLogic.class));
            String id = settings.getId().toString();
            cache.put(id, query);
            QueryExpirationProperties expiration = new QueryExpirationProperties();
            expiration.setIdleTimeout(-1);
            QueryExpirationBean expirationBean = new QueryExpirationBean();
            ReflectionTestUtils.setField(expirationBean, "queryCache", cache);
            ReflectionTestUtils.setField(expirationBean, "config", expiration);
            ReflectionTestUtils.setField(expirationBean, "connectionFactory", factory);
            ReflectionTestUtils.setField(expirationBean, "queryLogicCacheBean", qlCache);
            ReflectionTestUtils.setField(expirationBean, "metricsBean", metrics);
            ReflectionTestUtils.setField(expirationBean, "queryLimiter", queryLimiter);
            when(queryLimiter.getActiveQueries()).thenReturn(Collections.emptySet());
            AtomicBoolean firstReturn = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (firstReturn.compareAndSet(true, false)) {
                    throw new IOException("Expiration return failed");
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            try {
                expirationBean.removeIdleOrExpired();
                assertSame(query, cache.get(id), "Expiration forgot its failed-return cleanup owner");
                assertEquals(1, pool.getNumActive());
                expirationBean.removeIdleOrExpired();
                assertNull(cache.get(id));
                assertEquals(0, pool.getNumActive());
            } finally {
                returnTrackedClients(pool, client);
            }
        }
    }

    /**
     * Failed initialization detaches its client before reset calls the factory directly. Unlike close's return, that return was not counted as pending, so
     * administrative close evicted the cleanup owner while the pool still tracked its loan. If the return then failed, reset only logged it and leaked the
     * client. Hold that cleanup return across the real administrative API; keep its owner cached until success or retry has released the loan.
     *
     * @param beforeInitialization
     *            fail in accounting before setClient(), rather than inside initialize()
     * @param failReturn
     *            fail the first cleanup return before the real pool releases its loan; success controls pending-return retention alone
     */
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testRemovalRetainsFailedInitializationReturn(boolean beforeInitialization, boolean failReturn) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            QueryLogic<Object> logic = mock(QueryLogic.class);
            RunningQuery query = queryForReset(null, settings, logic);
            configureResetOwner(factory);
            String id = settings.getId().toString();
            cache.put(id, query);
            if (beforeInitialization) {
                when(logic.isQueryLimiterEnabled()).thenReturn(true);
                when(queryLimiter.checkForLimits(any(), any(), any())).thenReturn(QueryLimiterResponse.hasNotMetLimit());
                doThrow(new IOException("Accounting failed before initialization")).when(queryLimiter).countQueryTowardsLimits(any(), any(), any(), any());
            } else {
                when(logic.initialize(any(), any(), any())).thenThrow(new IOException("Initialization failed"));
            }
            CountDownLatch returning = new CountDownLatch(1);
            CountDownLatch releaseReturn = new CountDownLatch(1);
            AtomicBoolean firstReturn = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (firstReturn.compareAndSet(true, false)) {
                    returning.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS), "Cleanup return was not released");
                    if (failReturn) {
                        throw new IOException("Cleanup return failed before releasing its loan");
                    }
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                Future<?> reset = requests.submit(() -> bean.reset(id));
                assertTrue(returning.await(2, TimeUnit.SECONDS), "Reset did not reach its failure-cleanup return");
                bean.adminClose(id);
                // Capture the cache state before releasing the return; eventual success must not hide eviction of the pending owner.
                boolean retainedWhileReturning = cache.get(id) == query;
                releaseReturn.countDown();
                ExecutionException failure = assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                assertTrue(retainedWhileReturning, "Removal evicted a query whose initializer cleanup still owned a pending loan");
                assertEquals(0, pool.getNumActive(), "Removal/reset failure cleanup orphaned the loan");
                assertFalse(pool.connectorCameFromHere(client));
                assertNull(query.getClient());
                assertNull(cache.get(id));
            } finally {
                releaseReturn.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
            }
        }
    }

    /**
     * A failed cleanup return can occur without administrative removal. Keep that loan attached and cached so a later close can retry it. The accounting case
     * first uses and returns the identical physical client in an earlier execution: an unused reborrow is a new loan, even when its old initialization record
     * says the old loan was already returned. That retained client is for cleanup only; next must not reuse the previous execution's closed iterator.
     *
     * @param beforeInitialization
     *            whether accounting fails before initialization accepts the reborrowed client
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    public void testFailedInitializationReturnRemainsAvailableForRetry(boolean beforeInitialization) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            QueryLogic<Object> logic = mock(QueryLogic.class);
            RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
            query.closeConnection(factory);
            configureResetOwner(factory);
            String id = settings.getId().toString();
            cache.put(id, query);
            if (beforeInitialization) {
                when(logic.isQueryLimiterEnabled()).thenReturn(true);
                when(queryLimiter.checkForLimits(any(), any(), any())).thenReturn(QueryLimiterResponse.hasNotMetLimit());
                doThrow(new IOException("Accounting rejected a freshly reborrowed client")).when(queryLimiter).countQueryTowardsLimits(any(), any(), any(),
                                any());
            } else {
                when(logic.initialize(any(), any(), any())).thenThrow(new IOException("Replacement initialization failed"));
            }
            AtomicBoolean firstReturn = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (firstReturn.compareAndSet(true, false)) {
                    throw new IOException("Failed initializer's return failed");
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            try {
                assertThrows(DatawaveWebApplicationException.class, () -> bean.reset(id));
                assertSame(query, cache.get(id));
                assertSame(client, query.getClient(), "The failed cleanup return lost its retryable owner");
                assertEquals(1, pool.getNumActive());
                assertTrue(pool.connectorCameFromHere(client));
                assertThrows(QueryException.class, query::next, "A retained cleanup loan must not serve the old iterator");
                bean.adminClose(id);
                assertEquals(0, pool.getNumActive());
                assertFalse(pool.connectorCameFromHere(client));
                assertNull(query.getClient());
                assertNull(cache.get(id));
            } finally {
                returnTrackedClients(pool, client);
            }
        }
    }

    /**
     * A delayed closer removed limiter state by query ID even after instance-aware eviction protected a replacement RunningQuery. Use only real API calls to
     * replace the old object: a second administrative close evicts it, then reset loads a separate persisted definition, borrows the same pooled client, and
     * counts a new heartbeat. The older administrative/expiration cleanup must leave both that cached loan and its heartbeat intact.
     *
     * The earlier gate also covers a stale query lookup: the old object has been retrieved, but its cleanup has not yet captured accounting state. Checking
     * only a heartbeat snapshot without checking the cached instance could capture and stop the newer heartbeat at this boundary.
     *
     * @param expiration
     *            use scheduled expiration rather than administrative close for the older cleanup
     * @param beforeCleanup
     *            pause the older query's settings lookup before cleanup, rather than after its real connection cleanup
     */
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testDelayedRemovalPreservesReplacementHeartbeat(boolean expiration, boolean beforeCleanup) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(settings.getUserDN(), "CN=issuer"), UserType.USER, Arrays.asList("PRIVATE", "PUBLIC"),
                            null, null, 0L);
            DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
            settings.setOwner(principal.getShortName());
            when(ctx.getCallerPrincipal()).thenReturn(principal);
            String id = settings.getId().toString();
            QueryLogic<Object> oldLogic = mock(QueryLogic.class);
            when(oldLogic.isQueryLimiterEnabled()).thenReturn(true);
            RunningQuery oldQuery = spy(queryForReset(pool.borrowObject(new HashMap<>()), settings, oldLogic));
            cache.put(id, oldQuery);
            // Persistence returns a separate definition, without the old runtime handler that expiration records a timeout into.
            QueryImpl persisted = createNewQuery();
            persisted.setId(settings.getId());
            persisted.setOwner(principal.getShortName());
            QueryLogic<Object> replacementLogic = mock(QueryLogic.class);
            queryForReset(null, persisted, replacementLogic);
            when(replacementLogic.isQueryLimiterEnabled()).thenReturn(true);
            when(replacementLogic.getConnectionPriority()).thenReturn(AccumuloConnectionFactory.Priority.NORMAL);
            when(persister.findById(id)).thenReturn(Collections.singletonList(persisted));
            doReturn(replacementLogic).when(queryLogicFactory).getQueryLogic(persisted.getQueryLogicName(), principal);
            QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
            QueryHeartbeat originalHeartbeat = new QueryHeartbeat(id, Collections.emptyList());
            heartbeats.put(originalHeartbeat);
            QueryLimiter limiter = spy(new QueryLimiter());
            limiter.setHeartbeatCache(heartbeats);
            doReturn(QueryLimiterResponse.hasNotMetLimit()).when(limiter).checkForLimits(any(), any(), any());
            // Exercise real cache cleanup without contacting Zookeeper for node creation.
            doAnswer(invocation -> {
                heartbeats.put(new QueryHeartbeat(id, Collections.emptyList()));
                return null;
            }).when(limiter).countQueryTowardsLimits(any(), any(), any(), any());
            ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
            QueryExpirationProperties config = new QueryExpirationProperties();
            config.setIdleTimeout(1);
            ReflectionTestUtils.setField(oldQuery, "lastUsed", System.currentTimeMillis() - config.getIdleTimeoutMillis() - 1);
            QueryExpirationBean expirationBean = new QueryExpirationBean();
            ReflectionTestUtils.setField(expirationBean, "queryCache", cache);
            ReflectionTestUtils.setField(expirationBean, "config", config);
            ReflectionTestUtils.setField(expirationBean, "connectionFactory", factory);
            ReflectionTestUtils.setField(expirationBean, "queryLogicCacheBean", qlCache);
            ReflectionTestUtils.setField(expirationBean, "metricsBean", metrics);
            ReflectionTestUtils.setField(expirationBean, "queryLimiter", limiter);
            CountDownLatch olderPaused = new CountDownLatch(1);
            CountDownLatch releaseOlder = new CountDownLatch(1);
            AtomicBoolean firstCall = new AtomicBoolean(true);
            Answer<Object> pause = invocation -> {
                Object result = invocation.callRealMethod();
                if (firstCall.compareAndSet(true, false)) {
                    olderPaused.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseOlder, 5, TimeUnit.SECONDS), "Older cleanup was not released");
                }
                return result;
            };
            if (beforeCleanup) {
                doAnswer(pause).when(oldQuery).getSettings();
            } else {
                doAnswer(pause).when(oldQuery).closeConnection(factory);
            }
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                Future<?> removal = requests.submit(() -> {
                    if (expiration) {
                        expirationBean.removeIdleOrExpired();
                    } else {
                        bean.adminClose(id);
                    }
                });
                assertTrue(olderPaused.await(2, TimeUnit.SECONDS), "Older cleanup did not pause");
                bean.adminClose(id);
                assertNull(cache.get(id), "Second close did not evict the old object");
                bean.reset(id);
                RunningQuery replacement = cache.get(id);
                assertNotSame(oldQuery, replacement);
                assertSame(client, replacement.getClient());
                QueryHeartbeat replacementHeartbeat = heartbeats.get(id);
                assertNotNull(replacementHeartbeat, "Reset did not count its new execution");
                assertNotSame(originalHeartbeat, replacementHeartbeat);
                releaseOlder.countDown();
                removal.get(2, TimeUnit.SECONDS);
                assertSame(replacement, cache.get(id));
                assertEquals(1, pool.getNumActive());
                assertTrue(pool.connectorCameFromHere(client));
                assertSame(replacementHeartbeat, heartbeats.get(id), "Old cleanup stopped the replacement's heartbeat");
                bean.adminClose(id);
                assertNull(cache.get(id));
                assertNull(heartbeats.get(id));
                assertEquals(0, pool.getNumActive());
            } finally {
                releaseOlder.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
            }
        }
    }

    /**
     * Capturing the older heartbeat for close must still allow cleanup of a reset that starts counting a new execution on the same RunningQuery. Pause close
     * just before it marks removal, let reset borrow/count/install and block in initialize(), then complete close before initialization returns successfully.
     * That initializer cannot reopen the removed query; reset's final cleanup must stop the heartbeat it enrolled, even though no exception reaches reset.
     * Removal is terminal immediately, while cache eviction can wait until the admitted reset completes cleanup.
     *
     * This controls the former ID-based cleanup behavior while the replacement-instance tests require identity-aware cleanup.
     */
    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    public void testResetOvertakenByRemovalReleasesItsOwnHeartbeat() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            String id = settings.getId().toString();
            QueryLogic<Object> logic = mock(QueryLogic.class);
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            RunningQuery query = spy(queryForReset(pool.borrowObject(new HashMap<>()), settings, logic));
            cache.put(id, query);
            QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
            heartbeats.put(new QueryHeartbeat(id, Collections.emptyList()));
            QueryLimiter limiter = spy(new QueryLimiter());
            limiter.setHeartbeatCache(heartbeats);
            doReturn(QueryLimiterResponse.hasNotMetLimit()).when(limiter).checkForLimits(any(), any(), any());
            doAnswer(invocation -> {
                heartbeats.put(new QueryHeartbeat(id, Collections.emptyList()));
                return null;
            }).when(limiter).countQueryTowardsLimits(any(), any(), any(), any());
            ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
            CountDownLatch beforeRemoval = new CountDownLatch(1);
            CountDownLatch releaseRemoval = new CountDownLatch(1);
            CountDownLatch initializing = new CountDownLatch(1);
            CountDownLatch releaseInitialization = new CountDownLatch(1);
            doAnswer(invocation -> {
                beforeRemoval.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseRemoval, 5, TimeUnit.SECONDS), "Administrative removal was not released");
                return invocation.callRealMethod();
            }).when(query).markForRemoval();
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseInitialization, 5, TimeUnit.SECONDS), "Reset initialization was not released");
                return mock(GenericQueryConfiguration.class);
            });
            ExecutorService requests = Executors.newFixedThreadPool(2);
            try {
                Future<?> close = requests.submit(() -> bean.adminClose(id));
                assertTrue(beforeRemoval.await(2, TimeUnit.SECONDS), "Close did not pause before marking removal");
                Future<?> reset = requests.submit(() -> bean.reset(id));
                assertTrue(initializing.await(2, TimeUnit.SECONDS), "Reset did not count and install its new execution");
                assertNotNull(heartbeats.get(id), "Reset did not enroll its heartbeat");
                releaseRemoval.countDown();
                close.get(2, TimeUnit.SECONDS);
                assertTrue(query.isMarkedForRemoval(), "Close must terminate the execution while reset still owns cleanup");
                assertEquals(0, pool.getNumActive());
                releaseInitialization.countDown();
                reset.get(2, TimeUnit.SECONDS);
                assertNull(heartbeats.get(id), "Terminated reset left its own heartbeat counted");
                assertNull(query.getClient());
                assertNull(cache.get(id));
                assertEquals(0, pool.getNumActive());
            } finally {
                releaseRemoval.countDown();
                releaseInitialization.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
            }
        }
    }

    /**
     * Handler renewal depended on an existing iterator. A defined query's first failed planning attempt had no iterator, so its retry reused the failed handler
     * and rejected healthy initialization. Even if the old report arrived only after retry succeeded, that shared handler poisoned the new execution. Preserve
     * the legitimate first-attempt handler, then isolate the retry from both already-recorded and late reports by the failed attempt.
     *
     * @param reportBeforeRetry
     *            record the old planning failure immediately, rather than reporting it after the healthy retry has initialized
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    public void testResetAfterFirstPlanningFailureRenewsHandler(boolean reportBeforeRetry) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            QueryUncaughtExceptionHandler originalHandler = new QueryUncaughtExceptionHandler();
            settings.setUncaughtExceptionHandler(originalHandler);
            QueryLogic<Object> logic = mock(QueryLogic.class);
            queryForReset(null, settings, logic);
            when(logic.getMaxWork()).thenReturn(-1L);
            when(logic.getMaxPageSize()).thenReturn(100);
            when(logic.getMaxResults()).thenReturn(100L);
            when(logic.getResultLimit(settings)).thenReturn(100L);
            RunningQuery query = new RunningQuery(null, AccumuloConnectionFactory.Priority.NORMAL, logic, settings, null, null, new QueryMetricFactoryImpl());
            String id = settings.getId().toString();
            cache.put(id, query);
            IOException planningFailure = new IOException("First planning attempt failed before creating an iterator");
            AtomicBoolean firstAttempt = new AtomicBoolean(true);
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                if (firstAttempt.compareAndSet(true, false)) {
                    assertSame(originalHandler, settings.getUncaughtExceptionHandler(), "The first attempt lost its legitimate handler");
                    if (reportBeforeRetry) {
                        originalHandler.uncaughtException(Thread.currentThread(), planningFailure);
                    }
                    throw planningFailure;
                }
                return mock(GenericQueryConfiguration.class);
            });
            try {
                assertThrows(DatawaveWebApplicationException.class, () -> bean.reset(id));
                assertNull(query.getTransformIterator(), "The failure must precede iterator installation");
                assertNull(query.getClient());
                assertEquals(0, pool.getNumActive());
                assertDoesNotThrow(() -> bean.reset(id), "Healthy retry rethrew the earlier planning failure");
                assertNotSame(originalHandler, settings.getUncaughtExceptionHandler(), "Retry still shares the failed attempt's handler");
                originalHandler.uncaughtException(Thread.currentThread(), planningFailure);
                assertNull(settings.getUncaughtExceptionHandler().getUncaughtException(), "Late old report poisoned the retry");
                assertEquals(Collections.singletonList("row"), query.next().getResults());
                bean.adminClose(id);
                assertEquals(0, pool.getNumActive());
            } finally {
                returnTrackedClients(pool, client);
            }
        }
    }

    private AccumuloClientPool poolWithClients(AccumuloClient... clients) {
        AtomicInteger created = new AtomicInteger();
        AccumuloClientPoolFactory poolFactory = new AccumuloClientPoolFactory("test", "test", "unused", "unused") {
            @Override
            public PooledObject<AccumuloClient> makeObject() {
                return new DefaultPooledObject<>(clients[created.getAndIncrement()]);
            }
        };
        AccumuloClientPool pool = new AccumuloClientPool(poolFactory);
        pool.setMaxTotal(clients.length);
        pool.setMaxIdle(clients.length);
        return pool;
    }

    private void returnTrackedClients(AccumuloClientPool pool, AccumuloClient... clients) {
        for (AccumuloClient client : clients) {
            if (pool.connectorCameFromHere(client)) {
                pool.returnObject(client);
            }
        }
    }

    /**
     * A reset can own a pool loan before setClient installs it. Closing the disconnected cached query during borrowing, limiter enrollment, or prediction
     * previously evicted its only reachable cleanup owner. If the unused client's return then failed twice, later API close could not find the retained loan.
     * Keep a real one-client pool, reject returns before they release that loan, and require API cleanup to recover once the factory becomes available.
     * Successful-return cases establish that admission ownership ends without leaving a stale cache entry or an active loan. Interrupted limiter enrollment
     * must follow the same ownership cleanup, since interruption can occur after borrowing but before installation.
     *
     * @param phase
     *            where reset pauses before client installation
     * @param failReturn
     *            whether unused-client returns fail until reset finishes
     */
    @ParameterizedTest
    @CsvSource({"borrow, true", "accounting, true", "prediction, true", "borrow, false", "accounting, false", "prediction, false",
            "interruptedAccounting, true", "interruptedAccounting, false"})
    @Timeout(15)
    public void removalRetainsUninstalledResetLoanUntilReturnCompletes(String phase, boolean failReturn) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(client);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        configureResetOwner(factory);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        QueryLogic<Object> logic = mock(QueryLogic.class);
        RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
        query.closeConnection(factory);
        String id = settings.getId().toString();
        cache.put(id, query);
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Runnable pause = () -> {
            paused.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS), "Reset was not released");
        };
        if ("borrow".equals(phase)) {
            doAnswer(invocation -> {
                Object borrowed = invocation.callRealMethod();
                pause.run();
                return borrowed;
            }).when(factory).getClient(any(), any(), any(), any(), any());
        } else if ("accounting".equals(phase) || "interruptedAccounting".equals(phase)) {
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            when(queryLimiter.checkForLimits(any(), any(), any())).thenReturn(QueryLimiterResponse.hasNotMetLimit());
            doAnswer(invocation -> {
                pause.run();
                if ("interruptedAccounting".equals(phase)) {
                    throw new InterruptedException("Limiter enrollment interrupted before client installation");
                }
                return null;
            }).when(queryLimiter).countQueryTowardsLimits(any(), any(), any(), any());
        } else {
            query.setPredictor(metric -> {
                pause.run();
                return Collections.emptySet();
            });
        }
        AtomicBoolean rejectReturns = new AtomicBoolean(failReturn);
        AtomicInteger failedReturns = new AtomicInteger();
        doAnswer(invocation -> {
            if (rejectReturns.get()) {
                failedReturns.incrementAndGet();
                throw new IOException("Unused client return failed before releasing its loan");
            }
            return invocation.callRealMethod();
        }).when(factory).returnClient(client);
        ExecutorService requests = Executors.newSingleThreadExecutor();
        try {
            Future<?> reset = requests.submit(() -> bean.reset(id));
            assertTrue(paused.await(2, TimeUnit.SECONDS), "Reset did not pause before client installation");
            assertNull(query.getClient());
            bean.adminClose(id);
            assertEquals(1, pool.getNumActive(), "Close must not return a borrower-owned client");
            release.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
            assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
            if (failReturn) {
                assertEquals(2, failedReturns.get(), "Both failed-reset cleanup and its removal retry should retain the loan");
                assertEquals(1, pool.getNumActive());
                assertSame(client, query.getClient());
                assertSame(query, cache.get(id), "The failed unused loan must remain reachable by API close");
                rejectReturns.set(false);
                bean.adminClose(id);
            }
            assertNull(cache.get(id));
            assertNull(query.getClient());
            assertEquals(0, pool.getNumActive());
            assertFalse(pool.connectorCameFromHere(client));
        } finally {
            release.countDown();
            requests.shutdownNow();
            assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
            returnTrackedClients(pool, client);
            pool.close();
            logic.close();
        }
    }

    /**
     * Reconciliation read an old query's cancellation and later stopped accounting by query ID. API cancel removes that query, and reset reloads a separate
     * persisted definition and starts a replacement with the same ID. Pause after the old cancellation is read, reset through the real API, then finish that
     * stale inspection. The replacement must keep both its cached heartbeat and its active limiter node.
     */
    @SuppressWarnings("unchecked")
    @Test
    @Timeout(15)
    public void reconciliationPreservesHeartbeatEnrolledByResetAfterCancellation() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        AccumuloClientPool pool = singleClientPool(client);
        AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
        configureResetOwner(factory);
        QueryImpl settings = createNewQuery();
        settings.setOwner("owner");
        String id = settings.getId().toString();
        QueryLogic<Object> logic = mock(QueryLogic.class);
        when(logic.isQueryLimiterEnabled()).thenReturn(true);
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(settings.getUserDN(), "CN=issuer"), DatawaveUser.UserType.USER,
                        Arrays.asList("PRIVATE", "PUBLIC"), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        settings.setOwner(principal.getShortName());
        when(ctx.getCallerPrincipal()).thenReturn(principal);
        RunningQuery query = spy(queryForReset(pool.borrowObject(new HashMap<>()), settings, logic));
        cache.put(id, query);
        QueryImpl persisted = createNewQuery();
        persisted.setId(settings.getId());
        persisted.setOwner(principal.getShortName());
        QueryLogic<Object> replacementLogic = mock(QueryLogic.class);
        queryForReset(null, persisted, replacementLogic);
        when(replacementLogic.isQueryLimiterEnabled()).thenReturn(true);
        when(replacementLogic.getConnectionPriority()).thenReturn(AccumuloConnectionFactory.Priority.NORMAL);
        when(persister.findById(id)).thenReturn(Collections.singletonList(persisted));
        doReturn(replacementLogic).when(queryLogicFactory).getQueryLogic(persisted.getQueryLogicName(), principal);
        QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
        heartbeats.put(new QueryHeartbeat(id, Collections.emptyList()));
        QueryLimiter limiter = spy(new QueryLimiter());
        limiter.setHeartbeatCache(heartbeats);
        doReturn(QueryLimiterResponse.hasNotMetLimit()).when(limiter).checkForLimits(any(), any(), any());
        AtomicBoolean newNodeClosed = new AtomicBoolean();
        PersistentNode node = mock(PersistentNode.class);
        when(node.getActualPath()).thenAnswer(invocation -> newNodeClosed.get() ? null : "/active/reset");
        doAnswer(invocation -> {
            newNodeClosed.set(true);
            return null;
        }).when(node).close();
        doAnswer(invocation -> {
            heartbeats.put(new QueryHeartbeat(id, Collections.singletonList(node)));
            return null;
        }).when(limiter).countQueryTowardsLimits(any(), any(), any(), any());
        ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
        QueryExpirationBean expiration = new QueryExpirationBean();
        ReflectionTestUtils.setField(expiration, "queryCache", cache);
        ReflectionTestUtils.setField(expiration, "queryLimiter", limiter);
        CountDownLatch inspecting = new CountDownLatch(1);
        CountDownLatch releaseFinished = new CountDownLatch(1);
        CountDownLatch canceledSnapshot = new CountDownLatch(1);
        CountDownLatch releaseCanceled = new CountDownLatch(1);
        doAnswer(invocation -> {
            inspecting.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releaseFinished, 5, TimeUnit.SECONDS), "Finished inspection was not released");
            return invocation.callRealMethod();
        }).when(query).isFinished();
        doAnswer(invocation -> {
            Object canceled = invocation.callRealMethod();
            canceledSnapshot.countDown();
            assertTrue(Uninterruptibles.awaitUninterruptibly(releaseCanceled, 5, TimeUnit.SECONDS), "Canceled snapshot was not released");
            return canceled;
        }).when(query).isCanceled();
        ExecutorService cleaner = Executors.newSingleThreadExecutor();
        try {
            Future<?> reconciliation = cleaner.submit(() -> ReflectionTestUtils.invokeMethod(expiration, "synchronizeActiveQueries"));
            assertTrue(inspecting.await(2, TimeUnit.SECONDS), "Reconciliation did not capture the active query ID");
            bean.adminCancel(id);
            releaseFinished.countDown();
            assertTrue(canceledSnapshot.await(2, TimeUnit.SECONDS), "Reconciliation did not snapshot cancellation");
            bean.reset(id);
            RunningQuery replacement = cache.get(id);
            assertTrue(replacement != null && replacement != query, "Reset did not reload a separate query object");
            QueryHeartbeat resetHeartbeat = heartbeats.get(id);
            assertTrue(resetHeartbeat != null, "Reset did not enroll a new heartbeat");
            releaseCanceled.countDown();
            reconciliation.get(2, TimeUnit.SECONDS);
            assertSame(replacement, cache.get(id));
            assertFalse(replacement.isCanceled());
            assertSame(resetHeartbeat, heartbeats.get(id), "Stale cancellation must not uncount the replacement execution");
            assertFalse(newNodeClosed.get(), "The reset execution's limiter node must remain active");
            assertEquals(1, pool.getNumActive());
        } finally {
            releaseFinished.countDown();
            releaseCanceled.countDown();
            cleaner.shutdownNow();
            assertTrue(cleaner.awaitTermination(3, TimeUnit.SECONDS), "Reconciliation did not finish");
            RunningQuery replacement = cache.get(id);
            if (replacement != null && replacement != query) {
                replacement.closeConnection(factory);
            }
            query.closeConnection(factory);
            returnTrackedClients(pool, client);
            pool.close();
            heartbeats.shutdown();
        }
    }

    /**
     * Creation kept a local client and an independent temporary-cache reference. Cancel/close/expiration could return that loan during initialization; a late
     * failure then returned another query's reborrowed loan, while late success published a query pointing at it. Hold construction or initialization, retire
     * through real cleanup APIs, and reborrow from a one-client pool. The creator must reject publication and must never release the other borrower's loan.
     *
     * @param cleanup
     *            the cleanup path that overtakes creation
     * @param failInitialization
     *            whether initialization throws after cleanup
     * @param duringConstruction
     *            whether definition pre-initialization pauses before the RunningQuery constructor returns
     */
    @ParameterizedTest
    @CsvSource({"adminCancel, false, false", "adminCancel, true, false", "adminClose, false, false", "adminClose, true, false", "cancel, false, false",
            "cancel, true, false", "close, false, false", "close, true, false", "expiration, false, false", "expiration, true, false",
            "adminCancel, false, true", "adminClose, false, true"})
    @Timeout(15)
    public void creationCleanupDoesNotReturnAReborrowedLoan(String cleanup, boolean failInitialization, boolean duringConstruction) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            CountDownLatch initializing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean resourcesOpen = new AtomicBoolean();
            doAnswer(invocation -> {
                resourcesOpen.set(false);
                return null;
            }).when(logic).close();
            Answer<Object> pause = invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS), "Creation was not released");
                resourcesOpen.set(true);
                if (failInitialization) {
                    throw new IOException("Creation failed after its client was returned and reborrowed");
                }
                return mock(GenericQueryConfiguration.class);
            };
            if (duringConstruction) {
                doAnswer(invocation -> {
                    pause.answer(invocation);
                    return null;
                }).when(logic).preInitialize(any(), any());
            } else {
                when(logic.initialize(any(), any(), any())).thenAnswer(pause);
            }
            ExecutorService requests = Executors.newSingleThreadExecutor();
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(initializing.await(2, TimeUnit.SECONDS), "Creation did not reach its initialization gate");
                assertEquals(1, pool.getNumActive());
                cleanupCreation(cleanup, id, factory);
                assertEquals(0, pool.getNumActive(), "Cleanup did not return the creator's loan");
                AccumuloClient otherOwner = pool.borrowObject(new HashMap<>());
                assertSame(client, otherOwner);
                release.countDown();
                ExecutionException failure = assertThrows(ExecutionException.class, () -> create.get(2, TimeUnit.SECONDS),
                                "Retired creation must not publish an execution using an already returned client");
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                assertEquals(1, pool.getNumActive(), "Late creation cleanup returned another query's active loan");
                assertTrue(pool.connectorCameFromHere(otherOwner), "Late cleanup removed the other borrower's tracking");
                assertNull(cache.get(id));
                assertTrue(qlCache.snapshot().isEmpty(), "Completed creation cleanup retained its temporary owner");
                assertFalse(resourcesOpen.get(), "Resources acquired by the late initializer were not closed");
            } finally {
                release.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    /**
     * Normal creation is the ownership-transfer control: an accepted execution keeps exactly one tracked loan, releases its temporary initialization owner, and
     * returns that loan exactly once when closed. This exercises the same real creation path as the cancellation schedules.
     */
    @Test
    public void normalCreationTransfersItsLoanToTheCachedExecution() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            logicForCreation(settings);
            String id = settings.getId().toString();
            bean.createQuery(settings.getQueryLogicName(), createNewQueryParameterMap());
            assertSame(client, cache.get(id).getClient());
            assertEquals(1, pool.getNumActive());
            assertTrue(pool.connectorCameFromHere(client));
            assertTrue(qlCache.snapshot().isEmpty());
            bean.adminClose(id);
            assertEquals(0, pool.getNumActive());
            assertFalse(pool.connectorCameFromHere(client));
            assertNull(cache.get(id));
            verify(factory).returnClient(client);
        }
    }

    /**
     * A return failure must not discard creation's cleanup owner. Reject every return until the retired creator has finished, then recover the factory and
     * close through the API again. The old late-failure path retried the return but dropped the only temporary owner when that retry also failed.
     */
    @Test
    @Timeout(15)
    public void failedCreationReturnRemainsReachableAfterTheCreatorFinishes() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            CountDownLatch initializing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS));
                throw new IOException("Retired initializer failed while its unused return remained unavailable");
            });
            AtomicBoolean reject = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (reject.get()) {
                    throw new IOException("Return rejected before releasing the creator's loan");
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(initializing.await(2, TimeUnit.SECONDS));
                assertThrows(DatawaveWebApplicationException.class, () -> bean.adminCancel(id));
                release.countDown();
                ExecutionException failure = assertThrows(ExecutionException.class, () -> create.get(2, TimeUnit.SECONDS));
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                assertEquals(1, pool.getNumActive());
                assertTrue(pool.connectorCameFromHere(client));
                assertTrue(qlCache.snapshot().containsKey(id), "The failed creation loan lost its reachable cleanup owner");
                reject.set(false);
                bean.adminClose(id);
                assertEquals(0, pool.getNumActive());
                assertFalse(pool.connectorCameFromHere(client));
                assertTrue(qlCache.snapshot().isEmpty());
                assertNull(cache.get(id));
            } finally {
                release.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    /**
     * Reset enrolls accounting before setClient clears the old execution's finished flag. Reconciliation used that old flag to remove the fresh heartbeat, even
     * though it captured the right instance. Finish a real synchronous page using a null transformed row, pause reset after new enrollment, then require the
     * admitted reset to keep its heartbeat and actual node. Reconciliation after completed reset is the existing healthy-execution control.
     *
     * @param reconcileAfterReset
     *            whether to reconcile only after initialization completes
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(15)
    public void resetAccountingSurvivesReconciliationDuringEnrollment(boolean reconcileAfterReset) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            String id = settings.getId().toString();
            QueryLogic<Object> logic = mock(QueryLogic.class);
            when(logic.getResultLimit(any())).thenReturn(-1L);
            when(logic.getMaxResults()).thenReturn(-1L);
            when(logic.getMaxWork()).thenReturn(-1L);
            when(logic.getMaxPageSize()).thenReturn(100);
            when(logic.isUseSynchronousRunningQuery()).thenReturn(true);
            RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
            when(logic.getTransformIterator(settings)).thenReturn(new TransformIterator<>(Arrays.<Object> asList("old-row", null).iterator(), value -> value));
            query.setClient(client);
            assertEquals(Collections.singletonList("old-row"), query.next().getResults());
            assertTrue(query.isFinished());
            when(logic.getTransformIterator(settings)).thenReturn(new TransformIterator<>(Collections.singletonList("new-row").iterator(), value -> value));
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            cache.put(id, query);
            QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
            QueryLimiter limiter = spy(new QueryLimiter());
            limiter.setHeartbeatCache(heartbeats);
            doReturn(QueryLimiterResponse.hasNotMetLimit()).when(limiter).checkForLimits(any(), any(), any());
            AtomicBoolean nodeClosed = new AtomicBoolean();
            QueryHeartbeat heartbeat = heartbeatWithNode(id, nodeClosed);
            CountDownLatch counted = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(invocation -> {
                heartbeats.put(heartbeat);
                counted.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS));
                return null;
            }).when(limiter).countQueryTowardsLimits(any(), any(), any(), any());
            ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
            QueryExpirationBean expiration = accountingReconciliation(limiter);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                Future<?> reset = requests.submit(() -> bean.reset(id));
                assertTrue(counted.await(2, TimeUnit.SECONDS));
                assertTrue(query.isFinished(), "The test must observe the old terminal flags before client installation");
                if (reconcileAfterReset) {
                    release.countDown();
                    reset.get(2, TimeUnit.SECONDS);
                }
                ReflectionTestUtils.invokeMethod(expiration, "synchronizeActiveQueries");
                release.countDown();
                reset.get(2, TimeUnit.SECONDS);
                assertFalse(query.isFinished());
                assertEquals(Collections.singletonList("new-row"), query.next().getResults());
                assertSame(heartbeat, heartbeats.get(id), "Reconciliation stopped the new execution's startup accounting");
                assertFalse(nodeClosed.get());
                assertEquals(1, pool.getNumActive());
            } finally {
                release.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                query.closeConnection(factory);
                heartbeats.shutdown();
            }
        }
    }

    /**
     * A creating query has accounting but is not yet in the running cache. Reconciliation must recognize its temporary initialization owner instead of treating
     * its new heartbeat as orphaned. Hold initialization, reconcile while the running-cache entry is absent, then complete normal creation.
     */
    @Test
    @Timeout(15)
    public void creationAccountingSurvivesReconciliationBeforePublication() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
            QueryLimiter limiter = spy(new QueryLimiter());
            limiter.setHeartbeatCache(heartbeats);
            doReturn(QueryLimiterResponse.hasNotMetLimit()).when(limiter).checkForLimits(any(), any(), any());
            AtomicBoolean nodeClosed = new AtomicBoolean();
            QueryHeartbeat heartbeat = heartbeatWithNode(id, nodeClosed);
            doAnswer(invocation -> {
                heartbeats.put(heartbeat);
                return null;
            }).when(limiter).countQueryTowardsLimits(any(), any(), any(), any());
            ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
            CountDownLatch initializing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS));
                return mock(GenericQueryConfiguration.class);
            });
            ExecutorService requests = Executors.newSingleThreadExecutor();
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(initializing.await(2, TimeUnit.SECONDS));
                assertNull(cache.get(id));
                ReflectionTestUtils.invokeMethod(accountingReconciliation(limiter), "synchronizeActiveQueries");
                release.countDown();
                create.get(2, TimeUnit.SECONDS);
                assertSame(heartbeat, heartbeats.get(id), "The creating query lost accounting before it could be published");
                assertFalse(nodeClosed.get());
                bean.adminClose(id);
            } finally {
                release.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
                logic.close();
                heartbeats.shutdown();
            }
        }
    }

    /**
     * A failed factory return jumped over logic.close, leaving scanner/planner resources open on an already terminal query. Keep the failed loan reachable, but
     * close the logic immediately and retry only the loan once the factory recovers. Successful return is the matching cleanup control.
     *
     * @param failReturn
     *            whether the first return fails before releasing the loan
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void failedClientReturnStillClosesLogicResources(boolean failReturn) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            String id = settings.getId().toString();
            QueryLogic<Object> logic = mock(QueryLogic.class);
            RunningQuery query = queryForReset(pool.borrowObject(new HashMap<>()), settings, logic);
            cache.put(id, query);
            AtomicBoolean resourcesOpen = new AtomicBoolean(true);
            doAnswer(invocation -> {
                resourcesOpen.set(false);
                return null;
            }).when(logic).close();
            AtomicBoolean reject = new AtomicBoolean(failReturn);
            doAnswer(invocation -> {
                if (reject.get()) {
                    throw new IOException("Client return failed before release");
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            try {
                bean.adminClose(id);
                assertFalse(resourcesOpen.get(), "Factory failure bypassed query-logic cleanup");
                assertEquals(failReturn ? 1 : 0, pool.getNumActive());
                assertSame(failReturn ? query : null, cache.get(id));
                if (failReturn) {
                    assertSame(client, query.getClient());
                    reject.set(false);
                    bean.adminClose(id);
                    assertEquals(0, pool.getNumActive());
                    assertNull(cache.get(id));
                }
            } finally {
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    /**
     * Cleanup must find a creation before getClient returns its loan to the caller. Pause a real borrow after allocation but before assignment, close through
     * the API, then complete the borrow. The creator must return that unused loan instead of constructing and publishing an execution after cancellation.
     *
     * @param administrativeCancel
     *            use administrative cancellation rather than administrative close
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(15)
    public void creationCanceledDuringBorrowReturnsItsUnusedLoan(boolean administrativeCancel) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            CountDownLatch borrowed = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(invocation -> {
                Object loan = invocation.callRealMethod();
                borrowed.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS));
                return loan;
            }).when(factory).getClient(any(), any(), any(), any(), any());
            ExecutorService requests = Executors.newSingleThreadExecutor();
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(borrowed.await(2, TimeUnit.SECONDS));
                assertEquals(1, pool.getNumActive());
                assertDoesNotThrow(() -> cleanupCreation(administrativeCancel ? "adminCancel" : "adminClose", id, factory),
                                "Cleanup could not find the in-flight creation before borrowing completed");
                assertEquals(1, pool.getNumActive(), "Cleanup must not return a loan still owned by the borrowing call");
                release.countDown();
                ExecutionException failure = assertThrows(ExecutionException.class, () -> create.get(2, TimeUnit.SECONDS));
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                assertEquals(0, pool.getNumActive());
                assertFalse(pool.connectorCameFromHere(client));
                assertNull(cache.get(id));
                assertTrue(qlCache.snapshot().isEmpty());
                verify(factory).returnClient(client);
            } finally {
                release.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    /**
     * A creator can finish while administrative cancellation still owns a pending factory return. Finishing must neither claim a second return nor evict the
     * temporary owner before the first return completes. Hold the real factory boundary, fail the late initializer, then release that return and require the
     * cleanup owner and tracked loan to retire together.
     */
    @Test
    @Timeout(15)
    public void creationReturnPendingRetainsItsOwnerAfterTheCreatorFinishes() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            CountDownLatch initializing = new CountDownLatch(1);
            CountDownLatch releaseInitialization = new CountDownLatch(1);
            CountDownLatch returning = new CountDownLatch(1);
            CountDownLatch releaseReturn = new CountDownLatch(1);
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseInitialization, 5, TimeUnit.SECONDS));
                throw new IOException("Initializer failed while its earlier cancellation return was still pending");
            });
            AtomicBoolean firstReturn = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (firstReturn.compareAndSet(true, false)) {
                    returning.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseReturn, 5, TimeUnit.SECONDS));
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            ExecutorService requests = Executors.newFixedThreadPool(2);
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(initializing.await(2, TimeUnit.SECONDS));
                Future<?> cancel = requests.submit(() -> bean.adminCancel(id));
                assertTrue(returning.await(2, TimeUnit.SECONDS));
                releaseInitialization.countDown();
                ExecutionException failure = assertThrows(ExecutionException.class, () -> create.get(2, TimeUnit.SECONDS));
                assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                assertEquals(1, pool.getNumActive(), "The finished creator claimed the cancellation's pending return again");
                assertTrue(pool.connectorCameFromHere(client));
                assertTrue(qlCache.snapshot().containsKey(id), "Finishing evicted the pending return's cleanup owner");
                releaseReturn.countDown();
                cancel.get(2, TimeUnit.SECONDS);
                assertEquals(0, pool.getNumActive());
                assertFalse(pool.connectorCameFromHere(client));
                assertTrue(qlCache.snapshot().isEmpty());
                assertNull(cache.get(id));
                verify(factory).returnClient(client);
            } finally {
                releaseInitialization.countDown();
                releaseReturn.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    /**
     * A lookup that observed a cache miss could finish after creation published its live query. Its unconditional put replaced the sole owner of the creator's
     * pool loan with a disconnected definition; API close then returned no client. Hold the real persisted lookup, finish creation, and only then release it.
     * Administrative close starts while creation is still auditing, before temporary-cache registration, so it reaches the administrative persisted loader.
     * Reset's second borrow is rejected deliberately: the winner must return its old loan before that failure, rather than act on an uncached local definition.
     * The after-publication GET is the uncontended control. All cases use real cache and pool ownership, with client construction and persistence mocked.
     *
     * @param action
     *            the lookup caller
     * @param afterPublication
     *            whether GET begins only after creation has published
     */
    @ParameterizedTest
    @CsvSource({"get, false", "reset, false", "adminClose, false", "get, true"})
    @Timeout(15)
    public void persistedLookupPreservesTheQueryPublishedWhileItWasLoading(String action, boolean afterPublication) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            CountDownLatch creating = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            CountDownLatch loading = new CountDownLatch(1);
            CountDownLatch releaseLookup = new CountDownLatch(1);
            Answer<List<Query>> load = invocation -> {
                loading.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseLookup, 5, TimeUnit.SECONDS), "Persisted lookup was not released");
                return Collections.singletonList(settings);
            };
            when(persister.findById(id)).thenAnswer(load);
            when(persister.adminFindById(id)).thenAnswer(load);
            if ("adminClose".equals(action)) {
                when(logic.getAuditType(settings)).thenAnswer(invocation -> {
                    creating.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseCreation, 5, TimeUnit.SECONDS), "Creation audit was not released");
                    return AuditType.NONE;
                });
            } else {
                when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                    creating.countDown();
                    assertTrue(Uninterruptibles.awaitUninterruptibly(releaseCreation, 5, TimeUnit.SECONDS), "Creation initialization was not released");
                    return mock(GenericQueryConfiguration.class);
                });
            }
            if ("reset".equals(action)) {
                AtomicInteger borrows = new AtomicInteger();
                doAnswer(invocation -> {
                    if (borrows.incrementAndGet() > 1) {
                        throw new IOException("Reject the reset's second borrow before entering the one-client pool");
                    }
                    return invocation.callRealMethod();
                }).when(factory).getClient(any(), any(), any(), any(), any());
            }
            ExecutorService requests = Executors.newFixedThreadPool(2);
            RunningQuery published = null;
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(creating.await(2, TimeUnit.SECONDS));
                Future<?> lookup = afterPublication ? null : requests.submit(() -> {
                    if ("reset".equals(action)) {
                        return bean.reset(id);
                    } else if ("adminClose".equals(action)) {
                        return bean.adminClose(id);
                    } else {
                        return bean.get(id);
                    }
                });
                if (!afterPublication) {
                    assertTrue(loading.await(2, TimeUnit.SECONDS), "Lookup did not start loading the absent definition");
                }
                releaseCreation.countDown();
                create.get(2, TimeUnit.SECONDS);
                published = cache.get(id);
                assertNotNull(published);
                assertSame(client, published.getClient());
                assertEquals(1, pool.getNumActive());
                assertTrue(qlCache.snapshot().isEmpty(), "Successful creation must transfer its loan to the running cache");
                if (afterPublication) {
                    bean.get(id);
                } else {
                    releaseLookup.countDown();
                    if ("reset".equals(action)) {
                        Future<?> reset = lookup;
                        ExecutionException failure = assertThrows(ExecutionException.class, () -> reset.get(2, TimeUnit.SECONDS));
                        assertInstanceOf(DatawaveWebApplicationException.class, failure.getCause());
                    } else {
                        lookup.get(2, TimeUnit.SECONDS);
                    }
                }
                if ("adminClose".equals(action)) {
                    assertNull(cache.get(id));
                    assertNull(published.getClient(), "Administrative lookup closed a disconnected replacement instead of the published owner");
                } else {
                    assertSame(published, cache.get(id), "Delayed lookup replaced the live loan owner");
                    if ("reset".equals(action)) {
                        assertNull(published.getClient(), "Reset must use the winning cached query and retire its preceding loan");
                        assertEquals(0, pool.getNumActive(), "Reset ignored the cached winner and orphaned its loan");
                    }
                    bean.adminClose(id);
                }
                assertEquals(0, pool.getNumActive(), "API close could not reach the original creator's loan");
                assertFalse(pool.connectorCameFromHere(client));
                assertNull(cache.get(id));
            } finally {
                releaseCreation.countDown();
                releaseLookup.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                if (published != null) {
                    published.closeConnection(factory);
                }
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    /**
     * Interrupted reset initialization returned its client but skipped the ordinary failure path's limiter cleanup. Only metric lifecycle became CANCELLED, so
     * reconciliation still treated the disconnected cached query as active. Throw after enrollment from initialize or setup and verify the real heartbeat and
     * node are stopped even if the client return fails and remains owned for retry. IOException is the ordinary-failure control; the request must retain its
     * interruption flag. Cleanup assertions run after the real periodic reconciliation method.
     */
    @ParameterizedTest
    @CsvSource({"initialize, true, false", "setup, true, false", "initialize, false, false", "setup, false, false", "initialize, true, true",
            "setup, true, true"})
    @Timeout(15)
    public void interruptedResetStopsItsHeartbeatIndependentlyOfClientCleanup(String phase, boolean interrupted, boolean failReturn) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            String id = settings.getId().toString();
            QueryLogic<Object> logic = mock(QueryLogic.class);
            RunningQuery query = queryForReset(null, settings, logic);
            cache.put(id, query);
            when(logic.isQueryLimiterEnabled()).thenReturn(true);
            Answer<Object> failInitialization = invocation -> {
                if (interrupted) {
                    throw new InterruptedException("Initialization interrupted after heartbeat enrollment");
                }
                throw new IOException("Initialization failed after heartbeat enrollment");
            };
            if ("initialize".equals(phase)) {
                when(logic.initialize(any(), any(), any())).thenAnswer(failInitialization);
            } else {
                doAnswer(failInitialization).when(logic).setupQuery(any());
            }
            AtomicBoolean nodeClosed = new AtomicBoolean();
            QueryHeartbeat heartbeat = heartbeatWithNode(id, nodeClosed);
            QueryHeartbeatCache heartbeats = new QueryHeartbeatCache();
            QueryLimiter limiter = spy(new QueryLimiter());
            limiter.setHeartbeatCache(heartbeats);
            doReturn(QueryLimiterResponse.hasNotMetLimit()).when(limiter).checkForLimits(any(), any(), any());
            doAnswer(invocation -> {
                heartbeats.put(heartbeat);
                return null;
            }).when(limiter).countQueryTowardsLimits(any(), any(), any(), any());
            ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
            AtomicBoolean rejectReturn = new AtomicBoolean(failReturn);
            doAnswer(invocation -> {
                if (rejectReturn.get()) {
                    throw new IOException("Factory rejects return without releasing the loan");
                }
                return invocation.callRealMethod();
            }).when(factory).returnClient(client);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> reset = requests.submit(() -> {
                    assertThrows(DatawaveWebApplicationException.class, () -> bean.reset(id));
                    return Thread.currentThread().isInterrupted();
                });
                assertEquals(interrupted, reset.get(2, TimeUnit.SECONDS), "Reset lost or invented request interruption");
                assertSame(query, cache.get(id));
                assertEquals(failReturn ? 1 : 0, pool.getNumActive());
                assertSame(failReturn ? client : null, query.getClient());
                ReflectionTestUtils.invokeMethod(accountingReconciliation(limiter), "synchronizeActiveQueries");
                assertNull(heartbeats.get(id), "Failed reset still consumes a limiter slot after reconciliation");
                assertTrue(nodeClosed.get(), "Failed reset left its ephemeral limiter node open");
                rejectReturn.set(false);
                bean.adminClose(id);
                assertEquals(0, pool.getNumActive());
                assertNull(cache.get(id));
            } finally {
                rejectReturn.set(false);
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                query.closeConnection(factory);
                returnTrackedClients(pool, client);
                heartbeats.shutdown();
            }
        }
    }

    /**
     * QUERY_LOCKED_ERROR reached next's generic exception handler, which closed the cached query owned by the request holding that lock. The older lock test
     * cached no query and could only detect an accidental unlock. Retain a real loan and initialized query here: rejected pagination must preserve both as well
     * as the held lock. The unlocked control stubs only the page/response to avoid conflating admission with iterator or response-assembly failures.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void rejectedNextPreservesTheLockHoldersQueryAndClient(boolean locked) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            settings.setOwner("owner");
            String id = settings.getId().toString();
            QueryLogic<Object> logic = mock(QueryLogic.class);
            RunningQuery query = spy(queryForReset(pool.borrowObject(new HashMap<>()), settings, logic));
            cache.put(id, query);
            when(logic.getCollectQueryMetrics()).thenReturn(true);
            ResultsPage page = new ResultsPage(Collections.singletonList("row"), ResultsPage.Status.COMPLETE);
            doReturn(page).when(query).next();
            QueryLogicTransformer<Object,Object> transformer = mock(QueryLogicTransformer.class);
            when(logic.getEnrichedTransformer(settings)).thenReturn(transformer);
            when(transformer.createResponse(page)).thenReturn(new DefaultEventQueryResponse());
            when(responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
            if (locked) {
                assertTrue(cache.lock(id));
            }
            try {
                if (locked) {
                    assertThrows(DatawaveWebApplicationException.class, () -> bean.next(id));
                    assertFalse(cache.lock(id), "Rejected next released another request's lock");
                    verify(query, times(0)).next();
                } else {
                    assertDoesNotThrow(() -> bean.next(id));
                    assertTrue(cache.lock(id), "Successful next did not release its own lock");
                }
                assertSame(query, cache.get(id), "Admission rejection evicted another request's execution");
                assertSame(client, query.getClient());
                assertEquals(1, pool.getNumActive());
                assertTrue(pool.connectorCameFromHere(client));
                assertFalse(query.isMarkedForRemoval());
                assertNull(query.getMetric().getErrorMessage(), "Admission rejection modified the holder's query metric");
                verify(metrics, times(0)).updateMetric(any());
                verify(factory, times(0)).returnClient(client);
            } finally {
                cache.unlock(id);
                query.closeConnection(factory);
                returnTrackedClients(pool, client);
            }
        }
    }

    /**
     * Creation now holds the query ID lock through initialization, but a conflicting next still routed lock rejection through close(id). That cancelled the
     * temporary creation owner and returned its installed loan, so an otherwise successful initializer could not publish. Hold a real create call in its
     * initializer, reject concurrent pagination, then require creation to publish and ordinary close to return its loan exactly once.
     */
    @Test
    @Timeout(15)
    public void rejectedNextDoesNotCancelAnInitializingCreation() throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        try (AccumuloClientPool pool = singleClientPool(client)) {
            AccumuloConnectionFactoryImpl factory = factoryWithPool(pool);
            configureResetOwner(factory);
            QueryImpl settings = createNewQuery();
            QueryLogic<Object> logic = logicForCreation(settings);
            String id = settings.getId().toString();
            CountDownLatch initializing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            when(logic.initialize(any(), any(), any())).thenAnswer(invocation -> {
                initializing.countDown();
                assertTrue(Uninterruptibles.awaitUninterruptibly(release, 5, TimeUnit.SECONDS), "Creator was not released");
                return mock(GenericQueryConfiguration.class);
            });
            when(responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
            ExecutorService requests = Executors.newSingleThreadExecutor();
            MultivaluedMap<String,String> parameters = createNewQueryParameterMap();
            try {
                Future<?> create = requests.submit(() -> bean.createQuery(settings.getQueryLogicName(), parameters));
                assertTrue(initializing.await(2, TimeUnit.SECONDS));
                assertThrows(DatawaveWebApplicationException.class, () -> bean.next(id));
                assertEquals(1, pool.getNumActive(), "Lock rejection returned the creator's active loan");
                assertFalse(cache.lock(id), "Lock rejection released the creator's lock");
                assertTrue(qlCache.isInitializing(id), "Lock rejection cancelled the registered creation");
                release.countDown();
                assertDoesNotThrow(() -> create.get(2, TimeUnit.SECONDS), "Lock rejection must not make successful creation fail publication");
                assertSame(client, cache.get(id).getClient());
                bean.adminClose(id);
                assertEquals(0, pool.getNumActive());
                verify(factory).returnClient(client);
            } finally {
                release.countDown();
                requests.shutdownNow();
                assertTrue(requests.awaitTermination(3, TimeUnit.SECONDS));
                if (cache.get(id) != null) {
                    cache.get(id).closeConnection(factory);
                }
                returnTrackedClients(pool, client);
                logic.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private QueryLogic<Object> logicForCreation(QueryImpl settings) throws Exception {
        defineTestRunner(settings, createNewQueryParameterMap());
        QueryLogic<Object> logic = (QueryLogic<Object>) cache.get(settings.getId().toString()).getLogic();
        cache.remove(settings.getId().toString()); // Disconnected definition only: no execution or loan is released.
        settings.setOwner(((DatawavePrincipal) ctx.getCallerPrincipal()).getShortName());
        when(logic.getAuditType(any())).thenReturn(AuditType.NONE);
        when(logic.getMaxWork()).thenReturn(-1L);
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getTransformIterator(any())).thenReturn(new TransformIterator<>(Collections.singletonList("row").iterator(), value -> value));
        return logic;
    }

    private void cleanupCreation(String cleanup, String id, AccumuloConnectionFactory factory) {
        switch (cleanup) {
            case "adminCancel":
                bean.adminCancel(id);
                break;
            case "adminClose":
                bean.adminClose(id);
                break;
            case "cancel":
                bean.cancel(id);
                break;
            case "close":
                bean.close(id);
                break;
            case "expiration":
                ReflectionTestUtils.setField(qlCache, "connectionFactory", factory);
                qlCache.clearQueryLogics(Long.MAX_VALUE, 0);
                break;
            default:
                throw new IllegalArgumentException(cleanup);
        }
    }

    private QueryHeartbeat heartbeatWithNode(String id, AtomicBoolean closed) throws Exception {
        PersistentNode node = mock(PersistentNode.class);
        when(node.getActualPath()).thenAnswer(invocation -> closed.get() ? null : "/active/" + id);
        doAnswer(invocation -> {
            closed.set(true);
            return null;
        }).when(node).close();
        return new QueryHeartbeat(id, Collections.singletonList(node));
    }

    private QueryExpirationBean accountingReconciliation(QueryLimiter limiter) {
        QueryExpirationBean expiration = new QueryExpirationBean();
        ReflectionTestUtils.setField(expiration, "queryCache", cache);
        ReflectionTestUtils.setField(expiration, "queryLimiter", limiter);
        ReflectionTestUtils.setField(expiration, "queryLogicCacheBean", qlCache);
        return expiration;
    }

    private AccumuloClientPool singleClientPool(AccumuloClient client) {
        AccumuloClientPoolFactory poolFactory = new AccumuloClientPoolFactory("test", "test", "unused", "unused") {
            @Override
            public PooledObject<AccumuloClient> makeObject() {
                return new DefaultPooledObject<>(client);
            }
        };
        AccumuloClientPool pool = new AccumuloClientPool(poolFactory);
        pool.setMaxTotal(1);
        pool.setMaxIdle(1);
        return pool;
    }

    private AccumuloConnectionFactoryImpl factoryWithPool(AccumuloClientPool pool) {
        AccumuloConnectionFactoryImpl factory = mock(AccumuloConnectionFactoryImpl.class, CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(factory, "log", org.slf4j.LoggerFactory.getLogger(AccumuloConnectionFactoryImpl.class));
        ReflectionTestUtils.setField(factory, "defaultPoolName", "review");
        ReflectionTestUtils.setField(factory, "pools",
                        Collections.singletonMap("review", Collections.singletonMap(AccumuloConnectionFactory.Priority.NORMAL, pool)));
        return factory;
    }

    private void configureResetOwner(AccumuloConnectionFactory factory) throws Exception {
        UserTransaction transaction = mock(UserTransaction.class);
        when(transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(ctx.getUserTransaction()).thenReturn(transaction);
        when(ctx.getCallerPrincipal()).thenReturn(() -> "owner");
        ReflectionTestUtils.setField(bean, "connectionFactory", factory);
    }

    private RunningQuery queryForReset(AccumuloClient client, QueryImpl settings, QueryLogic<Object> logic) throws Exception {
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getTransformIterator(settings)).thenReturn(new TransformIterator<>(Collections.singletonList("row").iterator(), value -> value));
        when(logic.getAuditType(settings)).thenReturn(AuditType.NONE);
        return new RunningQuery(client, AccumuloConnectionFactory.Priority.NORMAL, logic, settings, null, null, new QueryMetricFactoryImpl());
    }

    private QueryImpl createNewQuery() throws Exception {
        Set<QueryImpl.Parameter> parameters = new HashSet<>();

        QueryImpl q = new QueryImpl();
        q.setBeginDate(beginDate);
        q.setEndDate(endDate);
        q.setExpirationDate(expirationDate);
        q.setPagesize(pagesize);
        q.setParameters(parameters);
        q.setQuery(query);
        q.setQueryAuthorizations(StringUtils.join(auths, ","));
        q.setQueryLogicName(queryLogicName);
        q.setUserDN(userDN);
        q.setDnList(Collections.singletonList(userDN));
        q.setId(UUID.randomUUID());

        return q;
    }

    private MultivaluedMap<String,String> createNewQueryParameterMap() throws Exception {
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putSingle(QueryParameters.QUERY_STRING, "foo == 'bar'");
        p.putSingle(QueryParameters.QUERY_NAME, "query name");
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(auths, ","));
        p.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        p.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        p.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "PRIVATE|PUBLIC");

        return p;
    }

    private MultivaluedMap<String,String> createNewQueryParameters(QueryImpl q, MultivaluedMap<String,String> p) {
        QueryParameters qp = new DefaultQueryParameters();
        MultivaluedMap<String,String> optionalParameters = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(p)));
        optionalParameters.putSingle(PrivateAuditConstants.USER_DN, userDN.toLowerCase());
        optionalParameters.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, "PRIVATE|PUBLIC");
        optionalParameters.putSingle(PrivateAuditConstants.LOGIC_CLASS, q.getQueryLogicName());

        return optionalParameters;
    }

    private void defineTestRunner(QueryImpl q, MultivaluedMap<String,String> p) throws Exception {

        MultivaluedMap<String,String> optionalParameters = createNewQueryParameters(q, p);

        @SuppressWarnings("rawtypes")
        QueryLogic logic = mock(BaseQueryLogic.class);

        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);

        when(ctx.getCallerPrincipal()).thenReturn(principal);
        when(persister.create(eq(principal.getUserDN().subjectDN()), eq(dnList), any(SecurityMarking.class), eq(queryLogicName), any(QueryParameters.class),
                        eq(optionalParameters))).thenReturn(q);
        when(queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn(logic);
        when(logic.getRequiredQueryParameters()).thenReturn(Collections.emptySet());
        when(logic.getConnectionPriority()).thenReturn(AccumuloConnectionFactory.Priority.NORMAL);
        when(logic.containsDNWithAccess(dnList)).thenReturn(true);
        when(logic.getMaxPageSize()).thenReturn(0);
        when(logic.getCollectQueryMetrics()).thenReturn(Boolean.FALSE);
        when(logic.getResultLimit(any())).thenReturn(-1L);
        when(logic.getMaxResults()).thenReturn(-1L);
        when(logic.getUserOperations()).thenReturn(null);
        when(responseObjectFactory.getQueryImpl()).thenReturn(new QueryImpl());

        bean.defineQuery(queryLogicName, p);

        Object cachedRunningQuery = cache.get(q.getId().toString());
        assertNotNull(cachedRunningQuery);
        RunningQuery rq2 = (RunningQuery) cachedRunningQuery;
        assertEquals(q, rq2.getSettings());

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateWithNoSelectedAuths() throws Exception {
        String queryLogicName = "EventQueryLogic";
        String queryName = "Something";
        String query = "FOO == BAR";
        Date beginDate = new Date();
        Date expirationDate = DateUtils.addDays(new Date(), 1);
        int pagesize = 10;
        QueryPersistence persist = QueryPersistence.TRANSIENT;
        Set<QueryImpl.Parameter> parameters = new HashSet<>();

        // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
        // setup principal mock
        String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
        String[] auths = new String[2];
        auths[0] = "PRIVATE";
        auths[1] = "PUBLIC";
        QueryImpl q = new QueryImpl();
        q.setBeginDate(beginDate);
        q.setEndDate(beginDate);
        q.setExpirationDate(expirationDate);
        q.setPagesize(pagesize);
        q.setParameters(parameters);
        q.setQuery(query);
        q.setQueryAuthorizations(StringUtils.join(auths, ","));
        q.setQueryLogicName(queryLogicName);
        q.setUserDN(userDN);
        q.setId(UUID.randomUUID());
        @SuppressWarnings("rawtypes")
        QueryLogic logic = mock(BaseQueryLogic.class);

        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, "");
        p.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        p.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "PRIVATE|PUBLIC");

        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        QueryParameters qp = new DefaultQueryParameters();
        MultivaluedMap<String,String> optionalParameters = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(p)));

        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);

        when(queryLimiter.checkForLimits(userDN.toLowerCase(), null, queryLogicName)).thenReturn(QueryLimiterResponse.hasNotMetLimit());
        when(ctx.getCallerPrincipal()).thenReturn(principal);
        when(persister.create(eq(userDN), eq(dnList), any(SecurityMarking.class), eq(queryLogicName), any(QueryParameters.class), eq(optionalParameters)))
                        .thenReturn(q);
        when(responseObjectFactory.getQueryImpl()).thenReturn(new QueryImpl());
        when(logic.getResultLimit(any())).thenReturn(-1L);
        when(queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn(logic);
        when(logic.getRequiredQueryParameters()).thenReturn(Collections.emptySet());
        when(logic.containsDNWithAccess(dnList)).thenReturn(true);
        when(logic.getMaxPageSize()).thenReturn(0);
        when(logic.getAuditType(any(Query.class))).thenReturn(AuditType.ACTIVE);
        when(logic.getSelectors(any())).thenReturn(null);
        Map<String,String> auditParams = new HashMap<>();
        auditParams.put(QueryParameters.QUERY_STRING, p.getFirst(QueryParameters.QUERY_STRING));
        auditParams.put(AuditParameters.USER_DN, userDN);
        auditParams.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "PRIVATE|PUBLIC");
        auditParams.put(AuditParameters.QUERY_AUDIT_TYPE, AuditType.ACTIVE.name());
        auditParams.put(AuditParameters.QUERY_LOGIC_CLASS, "EventQueryLogic");
        when(auditService.audit(eq(auditParams))).thenReturn(null);

        assertThrows(DatawaveWebApplicationException.class, () -> bean.createQuery(queryLogicName, p));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDefine() throws Exception {
        QueryImpl q = createNewQuery();
        MultivaluedMap<String,String> p = createNewQueryParameterMap();
        p.putSingle(QueryParameters.QUERY_LOGIC_NAME, "EventQueryLogic");

        defineTestRunner(q, p);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPredict() throws Exception {
        QueryImpl q = createNewQuery();
        MultivaluedMap<String,String> p = createNewQueryParameterMap();
        p.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);

        MultivaluedMap<String,String> optionalParameters = createNewQueryParameters(q, p);

        @SuppressWarnings("rawtypes")
        QueryLogic logic = mock(BaseQueryLogic.class);

        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);

        when(ctx.getCallerPrincipal()).thenReturn(principal);
        when(persister.create(eq(principal.getUserDN().subjectDN()), eq(dnList), any(SecurityMarking.class), eq(queryLogicName), any(QueryParameters.class),
                        eq(optionalParameters))).thenReturn(q);
        when(queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn(logic);
        when(logic.getRequiredQueryParameters()).thenReturn(Collections.emptySet());
        when(logic.containsDNWithAccess(dnList)).thenReturn(true);
        when(logic.getMaxPageSize()).thenReturn(0);

        Set<Prediction> predictions = new HashSet<>();
        predictions.add(new Prediction("source", 1));
        when(responseObjectFactory.getQueryImpl()).thenReturn(new QueryImpl());
        when(logic.getResultLimit(any())).thenReturn(-1L);

        // The bean builds its own BaseQueryMetric via metricFactory + populate(q). QueryMetric's no-arg
        // constructor stamps createDate from new Date() (truncated to the second), and createDate is part of
        // equals()/hashCode(). Comparing that internally-built metric against an independently hand-built one
        // via strict eq() is a real race: if the two constructions straddle a second boundary, the truncated
        // createDate values differ and eq() stops matching, intermittently. Pinning both sides' createDate to
        // the same value via reflection removes that race so eq() can be used safely.
        Date createDate = DateUtils.truncate(new Date(), Calendar.SECOND);
        QueryMetricFactory metricFactory = mock(QueryMetricFactory.class);
        when(metricFactory.createMetric()).thenAnswer(invocation -> {
            BaseQueryMetric m = new QueryMetric();
            ReflectionTestUtils.setField(m, "createDate", createDate);
            return m;
        });
        ReflectionTestUtils.setField(bean, "metricFactory", metricFactory);

        BaseQueryMetric expectedMetric = new QueryMetric();
        ReflectionTestUtils.setField(expectedMetric, "createDate", createDate);
        expectedMetric.populate(q);
        expectedMetric.setQueryType(RunningQuery.class.getSimpleName());
        when(predictor.predict(eq(expectedMetric))).thenReturn(predictions);

        GenericResponse<String> response = bean.predictQuery(queryLogicName, p);

        Object cachedRunningQuery = cache.get(q.getId().toString());
        assertNull(cachedRunningQuery);

        assertEquals(predictions.toString(), response.getResult());
    }

    // @Test
    @SuppressWarnings("unchecked")
    public void testGoodListWithGet() throws URISyntaxException, CloneNotSupportedException, ParserConfigurationException, IOException, SAXException {
        String queryName = "Something";
        // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
        // setup principal mock
        String dn = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
        String[] auths = new String[2];
        auths[0] = "PUBLIC";
        auths[1] = "PRIVATE";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(dn), UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        when(ctx.getCallerPrincipal()).thenReturn(principal);

        // setup persister with queries
        String logicName = "EventQuery";
        QueryImpl q1 = new QueryImpl();
        q1.setUserDN(principal.getShortName());
        q1.setQueryLogicName(logicName);
        q1.setQueryAuthorizations(auths.toString());
        q1.setId(new UUID(1, 1));
        QueryImpl q2 = new QueryImpl();
        q2.setUserDN(principal.getShortName());
        q2.setQueryLogicName(logicName);
        q2.setQueryAuthorizations(auths.toString());
        q2.setId(new UUID(1, 2));

        List<Query> queries = new ArrayList<>();
        queries.add(q1);
        queries.add(q2);
        when(persister.findByName(queryName)).thenReturn(queries);

        @SuppressWarnings("rawtypes")
        QueryLogic logic = mock(BaseQueryLogic.class);
        when(logic.getConnectionPriority()).thenReturn(AccumuloConnectionFactory.Priority.NORMAL);
        when(logic.getMaxPageSize()).thenReturn(0);

        when(queryLogicFactory.getQueryLogic(logicName, null)).thenReturn(logic);

        // setup test
        request = MockHttpRequest.get("/DataWave/Query/list?name=" + queryName);

        // execute
        dispatcher.invoke(request, response);

        // assert
        assertEquals(HttpServletResponse.SC_OK, response.getStatus());
        DocumentBuilder db = (DocumentBuilderFactory.newInstance()).newDocumentBuilder();
        Document doc = db.parse(new InputSource(new StringReader(response.getContentAsString())));
        NodeList returnedQueries = doc.getElementsByTagName("query");
        assertEquals(queries.size(), returnedQueries.getLength());
    }

    // @Test
    public void testListWithNoName() throws URISyntaxException {
        // setup test
        request = MockHttpRequest.get("/DataWave/Query/list");

        // execute
        dispatcher.invoke(request, response);

        // assert
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());
    }

    // @Test
    public void testListWithWrongUser() {

    }

    // @Test
    public void testListWhenQueryDoesNotExist() {

    }

    // @Test
    public void testListWithPost() {

    }

    private void nullDateTestRunner(boolean nullStart, boolean nullEnd) throws Exception {
        QueryImpl q = createNewQuery();
        MultivaluedMap<String,String> p = createNewQueryParameterMap();

        if (nullStart) {
            q.setBeginDate(null);
            p.remove(QueryParameters.QUERY_BEGIN);
            p.putSingle(QueryParameters.QUERY_BEGIN, null);
        }

        if (nullEnd) {
            q.setEndDate(null);
            p.remove(QueryParameters.QUERY_END);
            p.putSingle(QueryParameters.QUERY_END, null);
        }

        defineTestRunner(q, p);
    }

    @Test
    public void testBothDatesNull() throws Exception {
        nullDateTestRunner(true, true);
    }

    @Test
    public void testStartDateNull() throws Exception {
        nullDateTestRunner(true, false);
    }

    @Test
    public void testEndDateNull() throws Exception {
        nullDateTestRunner(false, true);
    }

    @Test
    public void testBeginDateAfterEndDate() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        final Date beginDate = sdf.parse("2024-01-02");
        final Date endDate = sdf.parse("2024-01-01");

        final MultivaluedMap<String,String> queryParameters = createNewQueryParameterMap();
        queryParameters.remove(QueryParameters.QUERY_BEGIN);
        queryParameters.remove(QueryParameters.QUERY_END);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));

        try {
            queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, "EventQueryLogic");
            bean.createQuery("EventQueryLogic", queryParameters);
            fail(); // If doesn't throw exception, should fail
        } catch (BadRequestException e) {
            assertEquals(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE.toString(), e.getCause().getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    @Timeout(5)
    public void testCloseActuallyCloses() throws Exception {
        QueryImpl q = createNewQuery();

        final MultivaluedMap<String,String> queryParameters = createNewQueryParameterMap();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, "EventQueryLogic");

        final Thread createQuery = new Thread(() -> {
            try {
                bean.createQuery("EventQueryLogic", queryParameters);
            } catch (Exception e) {
                // ok if we fail the call
                log.debug("createQuery terminated with " + e);
            }
        });

        final Throwable[] createQueryException = {null};
        createQuery.setUncaughtExceptionHandler((t, e) -> createQueryException[0] = e);

        @SuppressWarnings("rawtypes")
        QueryLogic logic = mock(BaseQueryLogic.class);

        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        principal.getShortName();
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);

        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient c = new InMemoryAccumuloClient("root", instance);

        MultivaluedMap<String,String> optionalParameters = createNewQueryParameters(q, queryParameters);

        when(queryLimiter.checkForLimits(userDN.toLowerCase(), null, queryLogicName)).thenReturn(QueryLimiterResponse.hasNotMetLimit());
        when(ctx.getCallerPrincipal()).thenReturn(principal);
        when(logic.getAuditType(null)).thenReturn(AuditType.NONE);
        when(persister.create(any(), any(), any(), any(), any(), any())).thenReturn(q);
        when(persister.findById(any(String.class))).thenReturn(null);
        when(responseObjectFactory.getQueryImpl()).thenReturn(new QueryImpl());
        when(logic.getResultLimit(any())).thenReturn(-1L);
        when(connectionFactory.getTrackingMap(any())).thenReturn(null);
        when(predictor.predict(any())).thenReturn(new HashSet<>());
        when(connectionFactory.getClient(eq(userDN.toLowerCase()), eq(null), eq("connPool1"), any(), any())).thenReturn(c);
        when(queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn(logic);
        when(logic.getRequiredQueryParameters()).thenReturn(Collections.emptySet());
        when(logic.getConnectionPriority()).thenReturn(AccumuloConnectionFactory.Priority.NORMAL);
        when(logic.containsDNWithAccess(dnList)).thenReturn(true);
        when(logic.getMaxPageSize()).thenReturn(0);
        when(logic.getAuditType(q)).thenReturn(AuditType.NONE);
        when(logic.getConnPoolName()).thenReturn("connPool1");
        when(logic.getMaxResults()).thenReturn(-1L);
        when(logic.getUserOperations()).thenReturn(null);
        when(connectionRequestBean.cancelConnectionRequest(any(), any())).thenReturn(false);
        when(logic.getCollectQueryMetrics()).thenReturn(Boolean.FALSE);

        final AtomicBoolean initializeLooping = new AtomicBoolean(false);

        // During initialize, mark that we get here, and then sleep
        final Answer<GenericQueryConfiguration> initializeAnswer = inv -> {
            initializeLooping.set(true);
            try {
                while (true) {
                    Thread.sleep(1000);
                    log.debug("Initialize: woke up");
                }
            } catch (InterruptedException e) {
                throw new QueryException("EXPECTED EXCEPTION: initialize interrupted");
            }
        };

        when(logic.initialize(any(AccumuloClient.class), any(Query.class), any(Set.class))).thenAnswer(initializeAnswer);

        // On close, interrupt the thread to simulate the ScannerFactory cleaning up
        final Answer<Object> closeAnswer = inv -> {
            if (null != createQuery) {
                log.debug("createQuery thread is not null. interrupting");
                createQuery.interrupt();
            } else {
                log.debug("createQuery thread is null. not interrupting");
            }
            return null;
        };

        doAnswer(closeAnswer).when(logic).close();
        try {
            createQuery.start();

            // Wait for the create call to get to initialize
            while (!initializeLooping.get()) {
                if (!createQuery.isAlive() && !initializeLooping.get()) {
                    fail("createQuery thread died before reaching initialize: " + createQueryException[0]);
                }
                Thread.sleep(50);
            }

            // initialize has not completed yet so it will not appear in the cache
            Object cachedRunningQuery = cache.get(q.getId().toString());
            assertNull(cachedRunningQuery);
            // Inspect without claiming the creation's cleanup ownership or manufacturing a second cache entry.
            Pair<QueryLogic<?>,AccumuloClient> pair = qlCache.snapshot().get(q.getId().toString());
            assertNotNull(pair);
            assertEquals(logic, pair.getLeft());
            assertEquals(c, pair.getRight());

            // Call close
            bean.close(q.getId().toString());

            // The owner may remain while creation acknowledges cancellation. Require retirement after the creator finishes cleanup.
            createQuery.join();
            assertFalse(qlCache.snapshot().containsKey(q.getId().toString()), "Completed creation left its temporary cleanup owner cached");

        } finally {
            if (null != createQuery && createQuery.isAlive()) {
                createQuery.interrupt();
            }
        }
    }
}
