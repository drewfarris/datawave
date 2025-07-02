package datawave.annotation.model;

import datawave.annotation.protobuf.SegmentData;
import datawave.data.hash.HashUID;
import datawave.data.hash.UID;

public class Segment {
    private UID segmentId;
    private final SegmentData segmentData;

    protected Segment(SegmentData segmentData) {
        this.segmentData = segmentData;
    }

    protected void generateUID() {
        if (segmentData == null) {
            throw new IllegalStateException("Can't generate uid because the segment data was null");
        }
        this.segmentId = HashUID.builder().newId(segmentData.toByteArray());
    }

    public UID getSegmentId() {
        if (segmentId == null) {
            throw new IllegalStateException("No UID has been generated, call generateUID() first");
        }
        return segmentId;
    }

    public SegmentData getSegmentData() {
        return segmentData;
    }

    public static Builder newBuilder() {
        return new Segment.Builder();
    }

    public static class Builder {

        SegmentData segmentData;

        protected Builder() {

        }

        public Builder setSegmentData(SegmentData segmentData) {
            this.segmentData = segmentData;
            return this;
        }

        public Segment build() {
            final Segment segment = new Segment(segmentData);
            segment.generateUID();
            return segment;
        }
    }
}
