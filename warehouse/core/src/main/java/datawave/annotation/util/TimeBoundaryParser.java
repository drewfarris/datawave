package datawave.annotation.util;

import datawave.annotation.model.TimeBounds;
import datawave.annotation.protobuf.SegmentBoundaryType;
import datawave.annotation.protobuf.SegmentBoundary;

public class TimeBoundaryParser {
    public static TimeBounds parse(SegmentBoundary source) {
        if (source.getType() != SegmentBoundaryType.TIME) {
            throw new IllegalArgumentException("cannot parse TimeBounds from source type: " + source.getType().name());
        }

        float start = Float.parseFloat(source.getStart());
        float end = Float.parseFloat(source.getEnd());

        return new TimeBounds(start, end);
    }

    public static SegmentBoundary encode(TimeBounds source) {
        return encode(SegmentBoundary.newBuilder(), source);
    }

    public static SegmentBoundary encode(SegmentBoundary.Builder builder, TimeBounds source) {
        builder.clear();

        builder.setType(SegmentBoundaryType.TIME);
        builder.setStart(Float.toString(source.getStart()));
        builder.setEnd(Float.toString(source.getEnd()));

        return builder.build();
    }
}
