package datawave.webservice.query.runner;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.data.UUIDType;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.cache.ClosedQueryCache;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.LookupUUIDUtil;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.util.FindAnnotation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ejb.EJBContext;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.util.*;

import static org.easymock.EasyMock.*;
import static org.powermock.reflect.Whitebox.invokeMethod;
import static org.powermock.reflect.Whitebox.setInternalState;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.security.auth.*", "com.sun.security.*"})
@PrepareForTest(FindAnnotation.class)
public class LookupUUIDQueryExecutorBeanTest {

    final AccumuloConnectionFactory accumuloConnectionFactory = new MockAccumuloConnectionFactory();

    @Mock
    LookupUUIDConfiguration configuration;

    @Mock
    EJBContext context;

    @Mock
    ClosedQueryCache closedCache;

    @Mock
    DatawavePrincipal principal;

    @Mock
    QueryCache cache;

    @Mock
    QueryLogicFactoryImpl queryLogicFactory;

    @Mock
    AuditBean auditor;

    @Mock
    UserOperations userOperations;

    final QueryExpirationProperties queryExpirationConf = new QueryExpirationProperties();
    final ResponseObjectFactory responseObjectFactory = new DefaultResponseObjectFactory();



    @Before
    public void before() {
        queryExpirationConf.setShortCircuitCheckTime(45);
        queryExpirationConf.setShortCircuitTimeout(58);
        queryExpirationConf.setIdleTimeout(60);
    }

    @Test
    public void testLookupUUIDQueryExecutorBean() throws Exception {
        UUIDType uuidType = PowerMock.createMock(UUIDType.class);
        BaseQueryResponse response = PowerMock.createMock(BaseQueryResponse.class);
        ManagedExecutorService executor = PowerMock.createMock(ManagedExecutorService.class);
        BaseQueryLogic<?> queryLogic = PowerMock.createMock(BaseQueryLogic.class);
        UriInfo uriInfo = PowerMock.createMock(UriInfo.class);
        HttpHeaders httpHeaders = PowerMock.createMock(HttpHeaders.class);

        MultivaluedHashMap<String, String> requestParams = new MultivaluedHashMap<>();
        requestParams.put("columnVisibility", List.of("PUBLIC"));
        expect(uriInfo.getQueryParameters()).andReturn(requestParams);

        expect(uuidType.getQueryLogic(null)).andReturn("abc");
        expect(response.getQueryId()).andReturn("11111");
        expect(context.getCallerPrincipal()).andReturn(principal).anyTimes();
        expect(executor.submit(isA(Runnable.class))).andReturn(null);

        MultivaluedMap<String,String> defaultParams = new MultivaluedMapImpl<>();
        defaultParams.putSingle("foo", "bar");
        defaultParams.putSingle("foo2", "default");

        expect(configuration.getContentLookupTypes()).andReturn(Collections.emptyMap()).anyTimes();
        expect(configuration.getUuidTypes()).andReturn(Collections.singletonList(new UUIDType("UUID", "LuceneUUIDEventQuery", 28))).anyTimes();
        expect(configuration.getBeginDate()).andReturn("20230101").anyTimes();
        expect(configuration.getBatchLookupUpperLimit()).andReturn(10).anyTimes();
        expect(configuration.getTagCloudLookupUpperLimit()).andReturn(50).anyTimes();
        expect(configuration.optionalParamsToMap()).andReturn(defaultParams).anyTimes();

        List<List<String>> auths = new ArrayList<>();
        auths.add(List.of("PUBLIC"));
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of("testUser", "testIssuer");

        expect(principal.getName()).andReturn("testUser").anyTimes();
        expect(principal.getShortName()).andReturn("testUser").anyTimes();
        expect(principal.getDNs()).andReturn(new String[]{ "testUser" } ).anyTimes();
        expect(principal.getUserDN()).andReturn(dnPair).anyTimes();
        expect(principal.getProxyServers()).andReturn(Collections.emptyList()).anyTimes();
        expect(principal.getAuthorizations()).andReturn((Collection) auths).anyTimes();

        expect(queryLogicFactory.getQueryLogic("LuceneUUIDEventQuery", principal)).andReturn((QueryLogic) queryLogic).anyTimes();

        queryLogic.preInitialize(anyObject(), anyObject());
        expectLastCall().anyTimes();

        expect(queryLogic.getUserOperations()).andReturn(userOperations).anyTimes();
        expect(queryLogic.containsDNWithAccess(anyObject())).andReturn(true).anyTimes();
        expect(queryLogic.getMaxPageSize()).andReturn(100).anyTimes();
        expect(queryLogic.getResultLimit(anyObject())).andReturn(100l).anyTimes();
        expect(queryLogic.getAuditType(anyObject())).andReturn(Auditor.AuditType.NONE).anyTimes();
        expect(queryLogic.getConnectionPriority()).andReturn(AccumuloConnectionFactory.Priority.LOW).anyTimes();
        expect(queryLogic.getConnPoolName()).andReturn("DEFAULT").anyTimes();
        expect(queryLogic.getCollectQueryMetrics()).andReturn(false).anyTimes();
        expect(queryLogic.getMaxResults()).andReturn(100l).anyTimes();
        expect(queryLogic.initialize(anyObject(), anyObject(), anyObject())).andReturn(new GenericQueryConfiguration(queryLogic)).anyTimes();

        expect(queryLogic.getRequiredQueryParameters()).andReturn(Collections.emptySet()).anyTimes();

        queryLogic.validate(anyObject(Map.class));
        expectLastCall().anyTimes();

        queryLogic.close();
        expectLastCall().anyTimes();

        expect((userOperations.getRemoteUser(principal))).andReturn(null).anyTimes();

        PowerMock.replayAll();

        QueryParameters queryParameters = new DefaultQueryParameters();
        Persister persister = new Persister();
        CreatedQueryLogicCacheBean qlCache = new CreatedQueryLogicCacheBean();
        QueryExecutorBean qeb = new QueryExecutorBean();
        LookupUUIDUtil utils = new LookupUUIDUtil(configuration, qeb, context, responseObjectFactory, queryLogicFactory, userOperations);
        AccumuloConnectionRequestBean connectionRequestBean = new AccumuloConnectionRequestBean();
        QueryTraceCache queryTraceCache = new QueryTraceCache();


        setInternalState(persister, EJBContext.class, context);
        setInternalState(persister, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(persister, AccumuloConnectionFactory.class, accumuloConnectionFactory);

        setInternalState(qlCache, AccumuloConnectionFactory.class, accumuloConnectionFactory);

        setInternalState(connectionRequestBean, EJBContext.class, context);

        invokeMethod(queryTraceCache, "init");

        setInternalState(qeb, EJBContext.class, context);
        setInternalState(qeb, QueryCache.class, cache);
        setInternalState(qeb, ClosedQueryCache.class, closedCache);
        setInternalState(qeb, Persister.class, persister);
        setInternalState(qeb, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(qeb, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(qeb, AuditBean.class, auditor);
        setInternalState(qeb, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(qeb, LookupUUIDUtil.class, utils);
        setInternalState(qeb, ManagedExecutorService.class, executor);
        setInternalState(qeb, QueryParameters.class, queryParameters);
        setInternalState(qeb, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(qeb, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(qeb, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(qeb, AccumuloConnectionFactory.class, accumuloConnectionFactory);
        setInternalState(qeb, AccumuloConnectionRequestBean.class, connectionRequestBean);
        setInternalState(qeb, QueryTraceCache.class, queryTraceCache);
        setInternalState(qeb, LookupUUIDConfiguration.class, configuration);

        qeb.init();
        qeb.lookupUUID("UUID", "1234567890", uriInfo, httpHeaders);

        PowerMock.verifyAll();
    }

    private static class MockAccumuloConnectionFactory implements AccumuloConnectionFactory {


        static FileSystem getFileSystem() {
            try {
                Configuration conf = new Configuration();
                conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
                conf.set("fs.default.name", "file:///");
                conf.set("hadoop.security.authentication", "simple");
                conf.set("hadoop.security.authorization", "false");
                return FileSystem.get(conf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private InMemoryInstance inMemoryInstance = new InMemoryInstance("testInstance", getFileSystem());

        public MockAccumuloConnectionFactory() {
            try {
                new InMemoryAccumuloClient("root", inMemoryInstance).securityOperations().changeUserAuthorizations("root", new Authorizations("PUB", "PVT"));
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, Priority priority, Map<String,String> trackingMap) throws Exception {
            return new InMemoryAccumuloClient("root", inMemoryInstance);
        }

        @Override
        public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, String poolName, Priority priority, Map<String,String> trackingMap)
                throws Exception {
            return new InMemoryAccumuloClient("root", inMemoryInstance);
        }

        @Override
        public void returnClient(AccumuloClient client) {

        }

        @Override
        public String report() {
            return null;
        }

        @Override
        public List<ConnectionPool> getConnectionPools() {
            return null;
        }

        @Override
        public int getConnectionUsagePercent() {
            return 0;
        }

        @Override
        public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
            return new HashMap<>();
        }

        @Override
        public void close() throws Exception {

        }
    }
}
