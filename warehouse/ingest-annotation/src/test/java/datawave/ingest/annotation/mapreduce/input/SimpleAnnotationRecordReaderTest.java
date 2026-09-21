package datawave.ingest.annotation.mapreduce.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.Test;

import datawave.ingest.data.TypeRegistry;

public class SimpleAnnotationRecordReaderTest {
    protected SimpleAnnotationRecordReader init(String inputData) throws Exception {
        Configuration conf = null;
        TaskAttemptContext ctx = null;
        InputSplit split = null;
        File dataFile = null;

        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/test-annotation-ingest-config.xml"));

        URL data = SimpleAnnotationRecordReaderTest.class.getResource(inputData);
        assertNotNull(data);

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        if ("file".equals(data.getProtocol())) {
            dataFile = new File(data.toURI());
        } else {
            dataFile = Files.createTempFile("annotation-record-reader-", ".json").toFile();
            dataFile.deleteOnExit();
            try (var input = data.openStream()) {
                Files.copy(input, dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        Path p = new Path(dataFile.toURI().toString());
        split = new FileSplit(p, 0, dataFile.length(), null);
        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        SimpleAnnotationRecordReader reader = new SimpleAnnotationRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }

    @Test
    public void testSingleAnnotation() throws Exception {
        SimpleAnnotationRecordReader sarr = init("/input/singleAnnotation.json");
        assertTrue(sarr.nextKeyValue());
        assertNotNull(sarr.getEvent().getRawData());
        assertFalse(sarr.nextKeyValue());
    }

    @Test
    public void testDoubleAnnotation() throws Exception {
        SimpleAnnotationRecordReader sarr = init("/input/doubleAnnotation.json");
        assertTrue(sarr.nextKeyValue());
        assertNotNull(sarr.getEvent().getRawData());
        assertTrue(sarr.nextKeyValue());
        assertNotNull(sarr.getEvent().getRawData());
        assertFalse(sarr.nextKeyValue());
    }

    @Test
    public void testFullAnnotationBaselineNdJson() throws Exception {
        SimpleAnnotationRecordReader reader = init("/annotation_baseline.ndjson");
        int recordCount = 0;

        while (reader.nextKeyValue()) {
            assertNotNull(reader.getEvent().getRawData());
            recordCount++;
        }

        assertEquals(36, recordCount);
    }
}
