package datawave.annotation.protobuf;

import org.junit.Test;

public class SegmentTest {
    @Test
    public void SimpleParseTest() {
        BoundsProtobuf.Builder b = BoundsProtobuf.newBuilder();
        b.setType(BoundType.TIME_RANGE);
        b.setStart("0.154");
        b.setEnd("0.52");

        AnnotationProtobuf.Builder annotationBuilder = AnnotationProtobuf.newBuilder();

        SegmentProtobuf.Builder segmentBuilder = SegmentProtobuf.newBuilder();
        segmentBuilder.setBounds(b.build());
        annotationBuilder.setValue("cow");
        annotationBuilder.setScore(.235f);
        segmentBuilder.addAnnotations(annotationBuilder.build());
        annotationBuilder.clear();
        annotationBuilder.setValue("horse");
        annotationBuilder.setScore(.21f);
        annotationBuilder.setExt("animal");
        segmentBuilder.addAnnotations(annotationBuilder.build());
        segmentBuilder.build();
    }
}
