package datawave.ingest.annotation.mapreduce.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
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
import datawave.util.time.DateHelper;

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

    protected InputSplit getSplit(String file) throws URISyntaxException, IOException {
        URL data = SimpleAnnotationIngestHelperTest.class.getResource(file);
        if (data == null) {
            File fileObj = new File(file);
            if (fileObj.exists()) {
                data = fileObj.toURI().toURL();
            }
        }
        assertNotNull(data, "Did not find test resource");

        File dataFile;
        if ("file".equals(data.getProtocol())) {
            dataFile = new File(data.toURI());
        } else {
            dataFile = Files.createTempFile("annotation-ingest-helper-", ".json").toFile();
            dataFile.deleteOnExit();
            try (var input = data.openStream()) {
                Files.copy(input, dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
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

    @Test
    public void testFullAnnotationBaselineHandlerAndDatatypeResolution() throws Exception {
        conf.set(TypeRegistry.INGEST_DATA_TYPES, "annotation,testDataType,wikipedia");
        conf.set("wikipedia.handler.classes", SimpleAnnotationDataTypeHandler.class.getName());
        conf.set(AnnotationHelper.ANNOTATION_REFERENCED_EVENT_DATATYPE_ALIASES, "enwiki:wikipedia,dewiki:wikipedia,eswiki:wikipedia,frwiki:wikipedia");
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        ingestHelper.setup(conf);
        AnnotationHelper annotationHelper = new AnnotationHelper(conf);

        split = getSplit("/annotation_baseline.ndjson");
        reader.initialize(split, ctx);
        reader.setInputDate(DateHelper.parse("20251001").getTime());

        Set<String> referencedDatatypes = new HashSet<>();
        int recordCount = 0;
        Annotation firstStoredAnnotation = null;
        while (reader.nextKeyValue()) {
            RawRecordContainer event = reader.getEvent();
            Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(event);
            assertFalse(event.fatalError());
            assertFalse(fields.isEmpty());
            assertEquals("annotation", event.getDataType().typeName());

            Annotation.Builder inputBuilder = Annotation.newBuilder();
            JsonFormat.parser().merge(new String(event.getRawData(), StandardCharsets.UTF_8), inputBuilder);
            Annotation inputAnnotation = inputBuilder.build();
            referencedDatatypes.add(inputAnnotation.getDataType());
            if (recordCount == 0) {
                assertEquals("C0CF2C89", inputAnnotation.getAnnotationId());
            }

            Annotation storedAnnotation = annotationHelper.buildAnnotation(event.getRawData(), inputAnnotation.getShard().getBytes(), event.getId(),
                            new ColumnVisibility("PUBLIC").flatten(), event);
            if (firstStoredAnnotation == null) {
                firstStoredAnnotation = storedAnnotation;
            }
            recordCount++;
        }

        assertEquals(36, recordCount);
        assertEquals(Set.of("enwiki", "dewiki", "eswiki", "frwiki"), referencedDatatypes);
        assertNotNull(firstStoredAnnotation);
        assertEquals("enwiki", firstStoredAnnotation.getDataType());
        assertEquals("shrgxu.x5rq5c.i3zexf", firstStoredAnnotation.getUid());
        assertEquals("tts", firstStoredAnnotation.getAnnotationType());
        assertEquals("inline v6", firstStoredAnnotation.getSource().getEngine());
        assertEquals("CC976C5F", firstStoredAnnotation.getAnnotationId());
    }
}
