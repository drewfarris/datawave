package datawave.annotation.data;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.model.Annotation;
import datawave.data.hash.UID;
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

import java.util.List;
import java.util.Map;

import static datawave.annotation.model.AnnotationTest.generateMultiTestSegment;
import static datawave.annotation.model.AnnotationTest.generateTestMetadata;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AccumuloAnnotationSerializerTest {

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

    public Annotation generateAnnotation() {
        return Annotation.newBuilder()
                .setSegments(List.of(generateMultiTestSegment()))
                .setMetadata(generateTestMetadata())
                .setShard("20250704_249")
                .setDataType("test")
                .setUid(UID.parse("abcde.fghij.klmno"))
                .build();
    }

    @Test
    public void testAnnotationSerializer() {
        Annotation a = generateAnnotation();
        AnnotationSerializer<List<Map.Entry<Key, Value>>> serializer = new AccumuloAnnotationSerializer();
        List<Map.Entry<Key, Value>> results = serializer.serialize(a);

        assertNotNull(results);

        results.forEach(e -> System.err.println(e.getKey() + " -> " + e.getValue()));

        // 3 rows - 1 for each metadata and one of the segment.
        assertEquals(3, results.size());

        for (Map.Entry<Key, Value> e: results) {
            Key key = e.getKey();
            Value value = e.getValue();

            assertEquals("Row id must be '20250704_249' but was " + key, key.getRow().toString());
            assertEquals("Column family must be 'test\0abcde.fghij.klmno\0test' but was " + key, key.getColumnFamily().toString());
            String cq = key.getColumnQualifier().toString();
            String[] parts = cq.split("\0");
            assertTrue("Column qualifier must have more than 2 parts: " + key, parts.length >= 2);
            String annotationType = parts[0];
            String annotationId = parts[1];
            assertEquals("Annotation type must be 'null', but was " + key, "null", annotationType);
            assertEquals("Annotation id must be 'comhxz.qyfmph.dpbt8m', but was " + key, "comhxz.qyfmph.dpbt8m", annotationId);
            if (parts.length == 3) {

            }
        }
    }
}
