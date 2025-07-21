package datawave.annotation.model;

import static datawave.annotation.util.AnnotationTestUtil.assertSegmentsEqual;
import static datawave.annotation.util.AnnotationTestUtil.generateTestMetadata;
import static datawave.annotation.util.AnnotationTestUtil.generateTestSegment;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import datawave.data.hash.UID;

public class AnnotationTest {

    @Test
    public void testAnnotationBuilder() {

        List<Segment> testSegments = List.of(generateTestSegment());
        Map<String,String> testMetadata = generateTestMetadata();

        Annotation a = Annotation.newBuilder().setShard("20240704_249").setDataType("testDocuments").setUid(UID.parse("abcd.efgh.ijkl"))
                        .setAnnotationType("correction").setAnnotationId(UID.parse("mnop.qrst.uvwx")).setMetadata(generateTestMetadata())
                        .setSegments(List.of(generateTestSegment())).build();

        assertEquals("20240704_249", a.getShard());
        assertEquals("testDocuments", a.getDataType());
        assertEquals("abcd.efgh.ijkl", a.getUid().toString());
        assertEquals("correction", a.getAnnotationType());
        assertEquals("mnop.qrst.uvwx", a.getAnnotationId().toString());
        assertEquals(testMetadata, a.getMetadata());
        assertSegmentsEqual(testSegments, a.getSegments());
    }

    @Test
    public void testAnnotationBuilderGeneratedUID() {

        List<Segment> testSegments = List.of(generateTestSegment());
        Map<String,String> testMetadata = generateTestMetadata();

        Annotation a = Annotation.newBuilder().setShard("20240704_249").setDataType("testDocuments").setUid(UID.parse("abcd.efgh.ijkl"))
                        .setAnnotationType("correction").setMetadata(generateTestMetadata()).setSegments(List.of(generateTestSegment())).build();

        assertEquals("20240704_249", a.getShard());
        assertEquals("testDocuments", a.getDataType());
        assertEquals("abcd.efgh.ijkl", a.getUid().toString());
        assertEquals("correction", a.getAnnotationType());
        assertEquals("kir5i4.tf9ozi.-ji6i29", a.getAnnotationId().toString());
        assertEquals(testMetadata, a.getMetadata());
        assertSegmentsEqual(testSegments, a.getSegments());
    }
}
