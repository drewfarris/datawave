package datawave.ingest.annotation.mapreduce.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimap;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.ingest.annotation.mapreduce.input.SimpleAnnotationRecordReader;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;

public class SimpleAnnotationIngestHelperTest {
    protected SimpleAnnotationIngestHelper ingestHelper;
    protected SimpleAnnotationRecordReader reader;
    protected Configuration conf;
    protected TaskAttemptContext ctx = null;
    protected InputSplit split = null;

    @BeforeEach
    public void setupIngestHelper() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/test-annotation-ingest-config.xml"));

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        ingestHelper = new SimpleAnnotationIngestHelper();
        ingestHelper.setup(conf);

        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        reader = new SimpleAnnotationRecordReader();
    }

    protected InputSplit getSplit(String file) throws URISyntaxException, MalformedURLException {
        URL data = SimpleAnnotationIngestHelperTest.class.getResource(file);
        if (data == null) {
            File fileObj = new File(file);
            if (fileObj.exists()) {
                data = fileObj.toURI().toURL();
            }
        }
        assertNotNull(data, "Did not find test resource");

        File dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        return new FileSplit(p, 0, dataFile.length(), null);
    }

    @Test
    public void testReferencedEventDatatypeDoesNotControlAnnotationProcessing() throws Exception {
        split = getSplit("/input/singleAnnotation.json");
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());

        assertTrue(reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        assertEquals("annotation", event.getDataType().typeName());
        assertEquals("myannotation", event.getDataType().outputName());

        Annotation annotation = Annotation.newBuilder().setDataType("testDataType").setUid("abcde.fghij.klmno").putMetadata("visibility", "PUBLIC")
                        .putMetadata("created_date", "2025-10-17T10:30:00.0Z").build();
        event.setRawData(JsonFormat.printer().print(annotation).getBytes(StandardCharsets.UTF_8));
        ingestHelper.getEventFields(event);

        assertEquals("annotation", event.getDataType().typeName());
        assertEquals("myannotation", event.getDataType().outputName());
    }

    @Test
    public void testExtractedFields() throws Exception {
        split = getSplit("/input/doubleAnnotation.json");
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());

        assertTrue(reader.nextKeyValue());
        RawRecordContainer e = reader.getEvent();

        assertEquals("myannotation", e.getDataType().outputName());
        assertNotNull(e.getRawData());
        assertFalse(e.fatalError());

        Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(e);
        assertTrue(reader.nextKeyValue());
        e = reader.getEvent();

        assertEquals("myannotation", e.getDataType().outputName());
        assertNotNull(e.getRawData());
        assertFalse(e.fatalError());

        assertFalse(reader.nextKeyValue());
    }
}
