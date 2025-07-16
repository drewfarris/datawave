package datawave.annotation.data;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.model.Annotation;
import datawave.annotation.model.Segment;
import datawave.annotation.protobuf.SegmentData;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static datawave.annotation.util.AnnotationTestUtil.assertMetadataEqual;
import static datawave.annotation.util.AnnotationTestUtil.assertSegmentsEqual;
import static datawave.annotation.util.AnnotationTestUtil.generateTestAnnotation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AccumuloAnnotationSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSerializerTest.class);

    public static final String TABLE_NAME = "testAnnotations";
    protected AccumuloClient client;
    protected TableOperations tableOperations;

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException
    {
        InMemoryInstance i = new InMemoryInstance(this.getClass().toString());
        client = new InMemoryAccumuloClient("root", i);
        if (client.tableOperations().exists(TABLE_NAME))
            client.tableOperations().delete(TABLE_NAME);
        client.tableOperations().create(TABLE_NAME);
        tableOperations = client.tableOperations();
    }

    @Test
    public void testAnnotationSerializerDeserialize() throws AnnotationSerializationException, InvalidProtocolBufferException {
        Annotation testAnnotation = generateTestAnnotation();
        AnnotationSerializer<List<Map.Entry<Key, Value>>> serializer = new AccumuloAnnotationSerializer();
        List<Map.Entry<Key, Value>> results = serializer.serialize(testAnnotation);

        assertNotNull(results);

        Annotation observedAnnotation = serializer.deserialize(results);

        assertSerialization(testAnnotation, results);
        assertDeserialization(testAnnotation, observedAnnotation);
    }

    private void assertSerialization(Annotation expected, List<Map.Entry<Key, Value>> results) throws InvalidProtocolBufferException {
        results.forEach(e -> log.debug("Observed key: '{}'", e.getKey()));

        // 3 rows - 1 for each metadata and one of the segment.
        assertEquals(3, results.size());

        List<Map.Entry<String, String>> observedMetadata = new ArrayList<>();
        List<Segment> observedSegments = new ArrayList<>();

        for (Map.Entry<Key, Value> e: results) {
            Key key = e.getKey();
            log.debug("Iterated key: '{}'", e.getKey());
            Value value = e.getValue();

            assertEquals("Row id mismatch", "20250704_249", key.getRow().toString());
            assertEquals("Column family mismatch", "testDataType\0abcde.fghij.klmno\0testAnnotationType", key.getColumnFamily().toString());
            String cq = key.getColumnQualifier().toString();
            String[] parts = cq.split("\0");
            assertTrue("Column qualifier incorrect length", parts.length >= 2);
            String annotationId = parts[0];
            assertEquals("Annotation id mismatch", "kir5i4.tf9ozi.-ji6i29", annotationId);
            if (parts.length == 2) {
                String segmentId = parts[1];
                assertEquals("comhxz.qyfmph.dpbt8m", segmentId);

                // the value must be decode-able into SegmentData.
                SegmentData segmentData = SegmentData.parseFrom(value.get());
                observedSegments.add(Segment.newBuilder().setSegmentData(segmentData).build());

            }
            if (parts.length == 3) {
                observedMetadata.add(Map.entry(parts[1],parts[2]));
            }
        }

        assertSegmentsEqual(expected.getSegments(), observedSegments);
        assertMetadataEqual(expected.getMetadata(), observedMetadata);
    }

    private void assertDeserialization(Annotation t, Annotation a) {
        assertEquals(t.getShard(), a.getShard());
        assertEquals(t.getDataType(), a.getDataType());
        assertEquals(t.getUid().toString(), a.getUid().toString());
        assertEquals(t.getAnnotationType(), a.getAnnotationType());
        assertEquals(t.getAnnotationId().toString(), a.getAnnotationId().toString());
        assertEquals(t.getMetadata(), a.getMetadata());
        assertSegmentsEqual(t.getSegments(), a.getSegments());
    }
}
