package datawave.annotation.util;

import datawave.annotation.model.TimeBounds;
import datawave.annotation.protobuf.BoundType;
import datawave.annotation.protobuf.BoundsProtobuf;

public class TimeBoundsParser {
    public static TimeBounds parse(BoundsProtobuf source) {
        if (source.getType() != BoundType.TIME_RANGE) {
            throw new IllegalArgumentException("cannot parse TimeBounds from source type: " + source.getType().name());
        }

        float start = Float.parseFloat(source.getStart());
        float end = Float.parseFloat(source.getEnd());

        return new TimeBounds(start, end);
    }

    public static BoundsProtobuf encode(TimeBounds source) {
        return encode(BoundsProtobuf.newBuilder(), source);
    }

    public static BoundsProtobuf encode(BoundsProtobuf.Builder builder, TimeBounds source) {
        builder.clear();

        builder.setType(BoundType.TIME_RANGE);
        builder.setStart(Float.toString(source.getStart()));
        builder.setEnd(Float.toString(source.getEnd()));

        return builder.build();
    }
}
