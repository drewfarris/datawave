package datawave.webservice.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import javax.ejb.EJBContext;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.data.visibility.AnnotationVisibilityTransformer;
import datawave.annotation.data.visibility.DefaultAnnotationVisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationTestUtil;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.metrics.QueryMetricQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutor;
import datawave.webservice.query.runner.QueryExecutorBean;

@RunWith(Arquillian.class)
public class AnnotationManagerBeanFunctionalTest {
    protected static AccumuloClient client = null;

    private static final Logger log = Logger.getLogger(AnnotationManagerBeanFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    // used for writing data for specific tests
    protected static AnnotationDataAccess testDao;

    @Mock
    @Produces
    private static EJBContext ctx;

    @Mock
    @Produces
    private static AccumuloConnectionFactory connectionFactory;

    @Mock
    @Produces
    private static QueryExecutorBean queryExecutorBean;

    @Mock
    @Produces
    private static QueryLogicFactory queryLogicFactory;

    @Mock
    @Produces
    private static ResponseObjectFactory responseObjectFactory;

    @Mock
    @Produces
    private static UserOperations userOperations;

    @Mock
    private static AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Inject
    @SpringBean(name = "AnnotationManager")
    protected AnnotationManager annotationManager;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "annotationBeanRefContext.xml");

        //@formatter:off
        return ShrinkWrap.create(JavaArchive.class)
                .addPackages(true,
                        "org.apache.deltaspike",
                        "io.astefanutti.metrics.cdi",
                        "datawave.query",
                        "org.jboss.logging",
                        "datawave.webservice.query.result.event",
                        "datawave.webservice.annotation")
                .addClass(AccumuloConnectionFactory.class)
                .addClass(QueryExecutor.class)
                .addClass(QueryLogicFactory.class)
                .addClass(ResponseObjectFactory.class)
                .addClass(UserOperations.class)
                .addClass(AccumuloConnectionRequestBean.class)
                .addClass(AnnotationManager.class)
                .addClass(AnnotationManagerBean.class)
                .deleteClass(DefaultEdgeEventQueryLogic.class)
                .deleteClass(RemoteEdgeDictionary.class)
                .deleteClass(QueryMetricQueryLogic.class)
                .addAsManifestResource(new StringAsset(
                                "<alternatives>" +
                                        "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" +
                                        "</alternatives>"),
                        "beans.xml");
        //@formatter:on
    }

    @BeforeClass
    public static void setUp() throws Exception {

        QueryTestTableHelper queryTestTableHelper = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        client = queryTestTableHelper.client;

        String tableName = "annotations";
        TableOperations tops = client.tableOperations();
        tops.create("annotations");

        Authorizations authorizations = new Authorizations("PUBLIC");

        AnnotationVisibilityTransformer visibilityTransformer = new DefaultAnnotationVisibilityTransformer();
        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer);
        testDao = new AnnotationDataAccess(client, authorizations, tableName, annotationSerializer);

        Annotation testAnnotation = AnnotationTestUtil.generateTestAnnotation();
        testDao.save(testAnnotation);

        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        Authorizations auths = new Authorizations("ALL");
        /*
         * PrintUtility.printTable(client, auths, TableName.SHARD); PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
         * PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
         */

        PrintUtility.printTable(client, auths, tableName);

        ctx = EasyMock.createMock(EJBContext.class);
        Principal principal = new DatawavePrincipal("testuser");
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();

        connectionFactory = EasyMock.createMock(AccumuloConnectionFactory.class);
        EasyMock.expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(new HashMap<>()).anyTimes();
        EasyMock.expect(connectionFactory.getClient(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                        EasyMock.anyObject())).andReturn(client).anyTimes();

        queryExecutorBean = EasyMock.createMock(QueryExecutorBean.class);
        queryLogicFactory = EasyMock.createMock(QueryLogicFactory.class);
        responseObjectFactory = EasyMock.createMock(ResponseObjectFactory.class);
        userOperations = EasyMock.createMock(UserOperations.class);
        accumuloConnectionRequestBean = EasyMock.createMock(AccumuloConnectionRequestBean.class);

        EasyMock.replay(ctx, connectionFactory);
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);

        AnnotationManagerBean bean = (AnnotationManagerBean) annotationManager;
        bean.setEJBContext(ctx);
    }

    @Test
    public void testGetAllAnnotationTypesInternalId() {
        Response response = annotationManager.getAllAnnotationTypes("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");
        assertEquals(200, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(Set.class.isAssignableFrom(entity.getClass()));
        @SuppressWarnings("unchecked")
        Set<String> annotationTypeList = (Set<String>) entity;
        assertEquals(1, annotationTypeList.size());
        assertTrue(annotationTypeList.contains("testAnnotationType"));
    }

    @Test
    public void testGetAllAnnotationTypesMissingInternalId() {
        Response response = annotationManager.getAllAnnotationTypes("DOCUMENT", "20250704_249/testDataType/12345.67890.12345");
        assertEquals(404, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(String.class.isAssignableFrom(entity.getClass()));
        String errorResponse = (String) entity;
        assertTrue(errorResponse.contains("No annotation types found for identifier"));
        assertTrue(errorResponse.contains("20250704_249/testDataType/12345.67890.12345"));
    }

    @Test
    public void testGetAnnotationsForInternalId() {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        Response response = annotationManager.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");
        assertEquals(200, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(ArrayList.class.isAssignableFrom(entity.getClass()));
        @SuppressWarnings("unchecked")
        ArrayList<Annotation> annotationList = (ArrayList<Annotation>) entity;
        assertEquals(1, annotationList.size());
        Annotation a = annotationList.get(0);
        AnnotationTestUtil.assertAnnotationsEqual(expectedAnnotation, a);
    }

    @Test
    public void testGetAnnotationsForMissingInternalId() {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        Response response = annotationManager.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/12345.67890.12345");
        assertEquals(404, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(String.class.isAssignableFrom(entity.getClass()));
        String errorResponse = (String) entity;
        assertTrue(errorResponse.contains("No annotations found for identifier"));
        assertTrue(errorResponse.contains("20250704_249/testDataType/12345.67890.12345"));
    }

    @Test
    public void testGetAllAnnotationsByTypeInternalId() {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        // TODO: insert a second annotation for the same document with a different type?
        Response response = annotationManager.getAnnotationsByType("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "testAnnotationType");
        assertEquals(200, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(ArrayList.class.isAssignableFrom(entity.getClass()));
        @SuppressWarnings("unchecked")
        ArrayList<Annotation> annotationList = (ArrayList<Annotation>) entity;
        assertEquals(1, annotationList.size());
        Annotation a = annotationList.get(0);
        AnnotationTestUtil.assertAnnotationsEqual(expectedAnnotation, a);
    }

    @Test
    public void testGetAllAnnotationByTypeInternalIdMissingType() {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        // TODO: insert a second annotation for the same document with a different type?
        Response response = annotationManager.getAnnotationsByType("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "missingType");
        Object entity = response.getEntity();
        assertTrue(String.class.isAssignableFrom(entity.getClass()));
        String errorResponse = (String) entity;
        assertTrue(errorResponse.contains("No annotations of type found for identifier"));
        assertTrue(errorResponse.contains("20250704_249/testDataType/abcde.fghij.klmno"));
        assertTrue(errorResponse.contains("missingType"));
    }

    @Test
    public void testGetAnnotationInternalId() throws Exception {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        Response response = annotationManager.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "bcb2bb84");
        assertEquals(200, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(Optional.class.isAssignableFrom(entity.getClass()));
        @SuppressWarnings("unchecked")
        Optional<Annotation> annotationOptional = (Optional<Annotation>) entity;
        assertFalse(annotationOptional.isEmpty());
        Annotation a = annotationOptional.get();
        AnnotationTestUtil.assertAnnotationsEqual(expectedAnnotation, a);
    }

    @Test
    public void testGetAnnotationMissingInternalId() throws Exception {
        Response response = annotationManager.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "aaaaaaaa");
        assertEquals(404, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(String.class.isAssignableFrom(entity.getClass()));
        String errorResponse = (String) entity;
        assertTrue(errorResponse.contains("No annotations found for identifier"));
        assertTrue(errorResponse.contains("20250704_249/testDataType/abcde.fghij.klmno"));
        assertTrue(errorResponse.contains("aaaaaaaa"));

    }

    @Test
    public void testUpdateAnnotationInternalId() {
        fail("Not implemented");
    }

    @Test
    public void testUpdateAnnotationInternalIdMissingId() {
        fail("Not implemented");
    }

    @Test
    public void testGetAnnotationSegmentInternalId() {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        Response response = annotationManager.getAnnotationSegment("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "bcb2bb84", "5a7bcdd9");
        assertEquals(200, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(Optional.class.isAssignableFrom(entity.getClass()));
        @SuppressWarnings("unchecked")
        Optional<Segment> segmentOptional = (Optional<Segment>) entity;
        assertFalse(segmentOptional.isEmpty());
        Segment s = segmentOptional.get();
        AnnotationTestUtil.assertSegmentsEqual(expectedAnnotation.getSegmentsList(), List.of(s));
    }

    @Test
    public void testGetAnnotationSegmentInternalIdMissingAnnotationId() {
        Response response = annotationManager.getAnnotationSegment("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "aaaaaaaa", "5a7bcdd9");
        assertEquals(404, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(String.class.isAssignableFrom(entity.getClass()));
        String errorResponse = (String) entity;
        assertTrue(errorResponse.contains("No annotations found for identifier"));
        assertTrue(errorResponse.contains("20250704_249/testDataType/abcde.fghij.klmno"));
        assertTrue(errorResponse.contains("aaaaaaaa"));
    }

    @Test
    public void testGetAnnotationSegmentInternalIdMissingSegmentId() {
        Response response = annotationManager.getAnnotationSegment("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "bcb2bb84", "bbbbbbbb");
        assertEquals(404, response.getStatus());
        Object entity = response.getEntity();
        assertTrue(String.class.isAssignableFrom(entity.getClass()));
        String errorResponse = (String) entity;
        assertTrue(errorResponse.contains("No segments found for identifier"));
        assertTrue(errorResponse.contains("20250704_249/testDataType/abcde.fghij.klmno"));
        assertTrue(errorResponse.contains("bbbbbbbb"));
    }

    @Test
    public void testAddSegmentInternalId() {
        fail("Not implemented");
    }

    @Test
    public void testUpdateSegmentInternalId() {
        fail("Not implemented");
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }
}
