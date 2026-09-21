package datawave.annotation.test.v1;

import static datawave.annotation.protobuf.v1.BoundaryType.ALL;
import static datawave.annotation.protobuf.v1.BoundaryType.POINTS;
import static datawave.annotation.protobuf.v1.BoundaryType.TEXT_CHAR;
import static datawave.annotation.protobuf.v1.BoundaryType.TIME_MILLI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.util.v1.AnnotationUtils;

/**
 * Various utility methods to generating test data. Generally the items created by utilities will not have identifiers injected so that they can be used to test
 * the data access objects.
 */
public class AnnotationTestDataUtil {
    public static final String CREATED_DATE = "2025-10-01T00:00:00Z";
    public static final String VISIBILITY = "PUBLIC";

    public static AnnotationSource generateTestAnnotationSource() {
        //@formatter:off
        return AnnotationSource.newBuilder()
                .setEngine("inline v6")
                .setModel("GR Supra")
                .setPlatform("toyota")
                .putMetadata("visibility", VISIBILITY)
                .putMetadata("created_date",CREATED_DATE)
                .putConfiguration("octane","99")
                .putConfiguration("model_year", "2025")
                .build();
        //@formatter:on
    }

    public static Annotation generateTestAnnotation() {
        //@formatter:off
        AnnotationSource partialSource = generateTestAnnotationSource();
        AnnotationSource source = AnnotationUtils.injectAnnotationSourceHashes(partialSource);

        return Annotation.newBuilder()
                .setShard("20250704_249")
                .setDataType("testDataType")
                .setUid("abcde.fghij.klmno")
                .setAnnotationType("testAnnotationType")
                .setDocumentId("1234567890")
                .setSource(source)
                .setAnalyticSourceHash(source.getAnalyticSourceHash())
                .addAllSegments(List.of(generateMultiTestSegment()))
                .putAllMetadata(generateTestAnnotationMetadata()).build();
        //@formatter:on
    }

    public static Map<String,String> generateTestAnnotationMetadata() {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        metadata.put("visibility", VISIBILITY);
        metadata.put("created_date", CREATED_DATE);
        return metadata;
    }

    public static Segment generateTestSegment() {
        SegmentValue segmentValue = SegmentValue.newBuilder().setValue("tree").setScore(.19f).build();
        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TIME_MILLI).setStart(1230).setEnd(1500).build();
        return Segment.newBuilder().addValues(segmentValue).setBoundary(bounds).build();
    }

    public static Segment generateMultiTestSegment() {
        SegmentValue segmentValueOne = SegmentValue.newBuilder().setValue("cow").setScore(.235f).build();
        Map<String,String> extension = new HashMap<>();
        extension.put("objectType", "animal");
        SegmentValue segmentValueTwo = SegmentValue.newBuilder().setValue("horse").setScore(.21f).putAllExtension(extension).build();
        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TIME_MILLI).setStart(1540).setEnd(5200).build();
        return Segment.newBuilder().addValues(segmentValueOne).addValues(segmentValueTwo).setBoundary(bounds).build();
    }

    public static List<Annotation> generateManyTestAnnotations() {
        List<Annotation> testAnnotations = new ArrayList<>();

        final String[] annotationTypes = {"tts", "tokens", "object", "image"};
        final EventReference[] eventReferences = {new EventReference("20130305_0", "enwiki", "shrgxu.x5rq5c.i3zexf"),
                new EventReference("20130305_0", "enwiki", "ibrtlu.qead3h.uz468c"), new EventReference("20250520_0", "dewiki", "-54aixo.k9pi3k.-fy30hf"),
                new EventReference("20250520_0", "dewiki", "kjxrup.gffwov.-sc1dcc"), new EventReference("20250520_0", "dewiki", "-yf39pt.fnsjk2.-c43q53"),
                new EventReference("20250520_0", "dewiki", "17wrdq.-azo85f.-w53xnp"), new EventReference("20250520_0", "dewiki", "-ltt8v2.-nmiz9z.-1vlors"),
                new EventReference("20250520_0", "eswiki", "lhaaph.-ld8sut.-yvw7r3"), new EventReference("20250520_0", "eswiki", "-ounwyg.-8dyou.-sq67x2"),
                new EventReference("20250520_0", "eswiki", "c3yao7.sdnsiw.-4nj1dq"), new EventReference("20250520_0", "eswiki", "9rytnl.nrnnhp.drlzp8"),
                new EventReference("20250520_0", "eswiki", "-8f46k2.-oi7pxl.iyt8va"), new EventReference("20250520_0", "eswiki", "9fw9d5.rlyjyn.urm0b9"),
                new EventReference("20250520_0", "eswiki", "-dsd7yq.khywox.nwewdq"), new EventReference("20250520_0", "frwiki", "sdnsxy.-p5rzxf.q66he6"),
                new EventReference("20250520_0", "frwiki", "-k9dr2z.-oskqjk.-b4ycxv"), new EventReference("20250520_0", "frwiki", "-bsep5q.hc13m7.qzpkyw"),
                new EventReference("20250520_0", "frwiki", "-9z02vc.-8s2x80.9zdl1a"), new EventReference("20250520_0", "frwiki", "um0ap3.-7cx9t4.-g8t81d")};

        AnnotationSource baseAnnotationSource = generateTestAnnotationSource();
        AnnotationSource annotationSource = AnnotationUtils.injectAnnotationSourceHashes(baseAnnotationSource);

        int documentId = 0;

        for (int i = 0; i < 36; i++) {
            EventReference eventReference = eventReferences[i % eventReferences.length];
            int segmentType = i % annotationTypes.length;

            //@formatter:off
            Annotation annotation = Annotation.newBuilder()
                    .setShard(eventReference.shard)
                    .setDataType(eventReference.dataType)
                    .setUid(eventReference.uid)
                    .setDocumentId(String.format("%012d", ++documentId))
                    .setAnalyticSourceHash(annotationSource.getAnalyticSourceHash())
                    .setSource(annotationSource)
                    .addAllSegments(generateTestSegments(eventReference.shard, eventReference.dataType, segmentType))
                    .putAllMetadata(generateTestMetadata(eventReference.shard.substring(0, 8), eventReference.shard, eventReference.dataType))
                    .setAnnotationType(annotationTypes[segmentType]).build();
            testAnnotations.add(annotation);
            //@formatter:on
        }

        return testAnnotations;
    }

    private static class EventReference {
        private final String shard;
        private final String dataType;
        private final String uid;

        private EventReference(String shard, String dataType, String uid) {
            this.shard = shard;
            this.dataType = dataType;
            this.uid = uid;
        }
    }

    public static List<AnnotationSource> generateManyTestAnnotationSources() {
        List<AnnotationSource> testAnnotationSources = new ArrayList<>();

        final String[] engines = {"v4", "v6", "v8"};
        final String[] models = {"camry", "corolla", "avalon"};
        final String[] sourceLabels = {"toyota", "honda", "mitsubishi"};
        final String[] configurations = {"circular", "reduction", "inherit", "standalone", "inline"};

        int iteration = 0;
        for (String engine : engines) {
            for (String model : models) {
                for (String sourceLabel : sourceLabels) {
                    int pos = iteration % configurations.length;
                    iteration++;
                    //@formatter:off
                    Map<String, String> metadata = Map.of(
                        "visibility", VISIBILITY,
                        "created_date", CREATED_DATE,
                        "provenance", engine + "/" + model
                    );

                    Map<String, String> configuration = Map.of(
                        "normalization", configurations[pos]
                    );
                    AnnotationSource annotationSource = AnnotationSource.newBuilder()
                            .setEngine(engine)
                            .setModel(model)
                            .setPlatform(sourceLabel)
                            .putAllMetadata(metadata)
                            .putAllConfiguration(configuration)
                            .build();
                    testAnnotationSources.add(annotationSource);
                    //@formatter:on
                }
            }
        }
        return testAnnotationSources;
    }

    public static List<Segment> generateTestSegments(String shard, String datatype, int segmentType) {
        switch (segmentType) {
            case 0:
                return generateAudioSegments(shard, datatype);
            case 1:
                return generateTextSegments(shard, datatype);
            case 2:
                return generateImageBoxSegments(shard, datatype);
            case 3:
                return generateImageAllSegments(shard, datatype);
            default:
                return List.of(generateMultiTestSegment());
        }
    }

    public static List<Segment> generateAudioSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();
        final String[] words = {"the", "cat", "sat", "on", "the", "mat", "<eos>"};
        final String[] altWords = {"the", "bat", "ate", "<unk>", "the", "gnat", "<eos>"};
        int wordPos = 0;

        // generate a boundary of 1 second of duration every 10 seconds
        for (int i = 0; i < 100; i += 10) {
            SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TIME_MILLI).setStart(i * 1000).setEnd((i + 5) * 1000).build();

            SegmentValue valueOne = SegmentValue.newBuilder().setValue(words[wordPos]).setScore(.235f).build();
            SegmentValue valueTwo = SegmentValue.newBuilder().setValue(altWords[wordPos]).setScore(.21f).build();
            Segment segment = Segment.newBuilder().setBoundary(bounds).addValues(valueOne).addValues(valueTwo).build();
            segments.add(segment);

            // cycle through words
            wordPos++;
            if (wordPos >= words.length) {
                wordPos = 0;
            }
        }
        return segments;
    }

    public static List<Segment> generateTextSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();
        final String[] words = {"the", "quick", "brown", "fox", "caught", "the", "rabbit", "<eos>"};
        int start = 0;
        for (String word : words) {
            int end = start + word.length();

            // character offsets
            SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TEXT_CHAR).setStart(start).setEnd(end).build();

            SegmentValue valueOne = SegmentValue.newBuilder().setValue(word).setScore(1.0f).build();
            Segment segment = Segment.newBuilder().setBoundary(bounds).addValues(valueOne).build();
            segments.add(segment);

            start = end + 1;
        }
        return segments;
    }

    public static List<Segment> generateImageBoxSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();

        final String[] objects = {"bird", "car", "stairs", "motorcycle", "flashlight", "dog"};
        final String[] altObjects = {"crow", "truck", "", "bicycle", "", "pig"};
        final String[] model = {"alpha", "beta", "delta", "beta", "beta", "alpha"};
        final int[][] upperLeft = {{0, 0}, {10, 15}, {20, 20}, {30, 50}, {60, 70}, {80, 90}};
        final int[][] lowerRight = {{5, 12}, {15, 18}, {28, 47}, {36, 55}, {70, 78}, {89, 95}};

        for (int i = 0; i < objects.length; i++) {
            Point topLeft = Point.newBuilder().setLabel("topLeft").setX(upperLeft[i][0]).setY(upperLeft[i][1]).build();
            Point bottomRight = Point.newBuilder().setLabel("bottomRight").setX(lowerRight[i][0]).setY(lowerRight[i][1]).build();
            SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(POINTS).addPoints(topLeft).addPoints(bottomRight).build();
            Segment.Builder segmentBuilder = Segment.newBuilder().setBoundary(bounds);

            Map<String,String> extension = new HashMap<>();
            extension.put("version", model[i]);
            segmentBuilder.addValues(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).putAllExtension(extension).build());
            if (!altObjects[i].isEmpty()) {
                segmentBuilder.addValues(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).putAllExtension(extension).build());
            }
            segments.add(segmentBuilder.build());
        }

        return segments;
    }

    public static List<Segment> generateImageAllSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();

        final String[] objects = {"landscape", "portrait", "scene", "evening", "astral", "material"};
        final String[] altObjects = {"underground", "postcard", "", "afternoon", "ethereal", "fabric"};
        final String[] model = {"charlie", "bravo", "lima", "victor", "delta", "micro"};

        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(ALL).build();
        Segment.Builder segmentBuilder = Segment.newBuilder().setBoundary(bounds);

        for (int i = 0; i < objects.length; i++) {
            Map<String,String> extension = new HashMap<>();
            extension.put("version", model[i]);
            segmentBuilder.addValues(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).putAllExtension(extension).build());
            if (!altObjects[i].isEmpty()) {
                segmentBuilder.addValues(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).putAllExtension(extension).build());
            }
        }

        segments.add(segmentBuilder.build());

        return segments;
    }

    public static Map<String,String> generateTestMetadata(String day, String shard, String datatype) {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("datatype", datatype);
        metadata.put("shard", shard);
        metadata.put("day", day);
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        metadata.put("visibility", VISIBILITY);
        metadata.put("created_date", CREATED_DATE);
        return metadata;
    }
}
