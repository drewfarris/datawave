package datawave.annotation.model;

import datawave.annotation.protobuf.SegmentBoundary;
import datawave.annotation.protobuf.SegmentBoundaryType;
import datawave.annotation.protobuf.SegmentData;
import datawave.annotation.protobuf.SegmentValue;
import datawave.data.hash.UID;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnnotationTest {

    public static Map<String, String> generateTestMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("foo","bar");
        metadata.put("plough", "plover");
        return metadata;
    }

    public static Segment generateTestSegment() {
        SegmentBoundary boundary = SegmentBoundary
                .newBuilder()
                .setType(SegmentBoundaryType.TIME)
                .setStart("0.154")
                .setEnd("0.52")
                .build();

        SegmentValue segmentValue = SegmentValue.newBuilder()
                .setValue("horse")
                .setScore(.21f)
                .setExtension("animal")
                .build();

        SegmentData segmentData = SegmentData.newBuilder()
                .addValue(segmentValue)
                .setBoundary(boundary)
                .build();

        return Segment.newBuilder().setSegmentData(segmentData).build();
    }

    public static Segment generateMultiTestSegment() {
        SegmentBoundary boundary = SegmentBoundary
                .newBuilder()
                .setType(SegmentBoundaryType.TIME)
                .setStart("0.154")
                .setEnd("0.52")
                .build();

        SegmentValue segmentValueOne = SegmentValue.newBuilder()
                .setValue("cow")
                .setScore(.235f)
                .build();

        SegmentValue segmentValueTwo = SegmentValue.newBuilder()
                .setValue("horse")
                .setScore(.21f)
                .setExtension("animal")
                .build();

        SegmentData segmentData = SegmentData.newBuilder()
                .addValue(segmentValueOne)
                .addValue(segmentValueTwo)
                .setBoundary(boundary)
                .build();

        return Segment.newBuilder()
                .setSegmentData(segmentData)
                .build();
    }

    @Test
    public void testAnnotationBuilder() {

        List<Segment> testSegments = List.of(generateTestSegment());
        Map<String, String> testMetadata = generateTestMetadata();

        Annotation a = Annotation.newBuilder()
                .setShard("20240704_249")
                .setDataType("testDocuments")
                .setUid(UID.parse("abcd.efgh.ijkl"))
                .setAnnotationType("correction")
                .setAnnotationId(UID.parse("mnop.qrst.uvwx"))
                .setMetadata(generateTestMetadata())
                .setSegments(List.of(generateTestSegment()))
                .build();

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
        Map<String, String> testMetadata = generateTestMetadata();

        Annotation a = Annotation.newBuilder()
                .setShard("20240704_249")
                .setDataType("testDocuments")
                .setUid(UID.parse("abcd.efgh.ijkl"))
                .setAnnotationType("correction")
                .setMetadata(generateTestMetadata())
                .setSegments(List.of(generateTestSegment()))
                .build();

        assertEquals("20240704_249", a.getShard());
        assertEquals("testDocuments", a.getDataType());
        assertEquals("abcd.efgh.ijkl", a.getUid().toString());
        assertEquals("correction", a.getAnnotationType());
        assertEquals("kir5i4.tf9ozi.-ji6i29", a.getAnnotationId().toString());
        assertEquals(testMetadata, a.getMetadata());
        assertSegmentsEqual(testSegments, a.getSegments());
    }

    public static void assertSegmentsEqual(List<Segment> expected, List<Segment> result) {
        Map<String, Segment> expectedByUID = indexSegments(expected);
        Set<String> expectedUIDs = expectedByUID.keySet();

        Map<String, Segment> resultByUID = indexSegments(result);
        Set<String> resultUIDs = resultByUID.keySet();

        List<String> missing = new ArrayList<>(expectedUIDs);
        missing.removeAll(resultUIDs);

        List<String> extra = new ArrayList<>(resultUIDs);
        extra.removeAll(expectedUIDs);

        List<String> mismatchMessages = new ArrayList<>();
        if (!missing.isEmpty()) {
            mismatchMessages.add("Results are missing expected uids: " + missing);
        }
        if (!extra.isEmpty()) {
            mismatchMessages.add("Results have extra uids: " + extra);
        }
        assertTrue("Mismatch in uuids observed: " + mismatchMessages, mismatchMessages.isEmpty());

        
        List<String> mismatchedSegmentMessages = new ArrayList<>();
        for (Map.Entry<String, Segment> expectedSegment: expectedByUID.entrySet()) {
            String expectedKey = expectedSegment.getKey();
            Segment resultSegment = resultByUID.get(expectedKey);
            compareSegments(expectedSegment, resultSegment, mismatchedSegmentMessages);
        }

        assertTrue("Segment mismatches observed: " + mismatchedSegmentMessages, mismatchedSegmentMessages.isEmpty());
    }

    public static Map<String, Segment> indexSegments(List<Segment> input) {
        final Map<String, Segment> index = new HashMap<>();
        for (Segment s: input) {
            index.put(s.getSegmentId().toString(), s);
        }
        return index;
    }

    public static void compareSegments(String expected, String result, List<String> mismatchedSegmentMessages) {

    }
}
