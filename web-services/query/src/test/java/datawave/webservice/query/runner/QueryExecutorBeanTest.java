package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
     * after acquiring a late resource. The other lease must remain active and the late resource must be reclaimed.
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
            assertNull(cache.get(id));
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
            Pair<QueryLogic<?>,AccumuloClient> pair = qlCache.poll(q.getId().toString());
            assertNotNull(pair);
            assertEquals(logic, pair.getLeft());
            assertEquals(c, pair.getRight());

            // Have to add these back because poll was destructive
            qlCache.add(q.getId().toString(), principal.getShortName(), pair.getLeft(), pair.getRight());

            // Call close
            bean.close(q.getId().toString());

            // Make sure that it's gone from the qlCache
            pair = qlCache.poll(q.getId().toString());
            assertNull(pair, "Still found an entry in the qlCache: " + pair);

            // Should have already joined by now, but just to be sure
            createQuery.join();

        } finally {
            if (null != createQuery && createQuery.isAlive()) {
                createQuery.interrupt();
            }
        }
    }
}
