package datawave.annotation.util;

import datawave.annotation.model.Point;
import datawave.annotation.model.PointBounds;
import datawave.annotation.protobuf.SegmentBoundaryType;
import datawave.annotation.protobuf.SegmentBoundary;

public class PositionBoundaryParser {
    public static PointBounds parse(SegmentBoundary source) {
        if (source.getType() != SegmentBoundaryType.POSITION) {
            throw new IllegalArgumentException("Cannot parse PointBounds from source type: " + source.getType().name());
        }

        Point start = parsePoint(source.getStart());
        Point end = parsePoint(source.getEnd());

        int rotation = 0;
        if (source.hasRotation()) {
            rotation = source.getRotation();
        }

        return new PointBounds(start, end, rotation);
    }

    public static Point parsePoint(String encoded) {
        String[] parts = encoded.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Points must be encoded as x,y");
        }
        float x = Float.parseFloat(parts[0]);
        float y = Float.parseFloat(parts[1]);

        return new Point(x, y);
    }

    public static String encodePoint(Point p) {
        return p.getX() + "," + p.getY();
    }

    public static SegmentBoundary encode(PointBounds source) {
        return encode(SegmentBoundary.newBuilder(), source);
    }

    public static SegmentBoundary encode(SegmentBoundary.Builder builder, PointBounds source) {
        builder.clear();

        builder.setType(SegmentBoundaryType.POSITION);
        builder.setStart(encodePoint(source.getTopLeft()));
        builder.setEnd(encodePoint(source.getBottomRight()));
        if (source.getRotation() != 0) {
            builder.setRotation(source.getRotation());
        }

        return builder.build();
    }
}
