package datawave.annotation.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.data.hash.HashUID;
import datawave.data.hash.UID;

public class Annotation {
    private final String eventShard;
    private final String eventDataType;
    private final UID eventUID;
    private UID annotationId;

    private final String anotationType;
    private final Map<String,String> metadata;

    private final List<Segment> segments;

    protected Annotation(String eventShard, String eventDataType, UID eventUID, String anotationType, Map<String, String> metadata, List<Segment> segments) {
        this.eventShard = eventShard;
        this.eventDataType = eventDataType;
        this.eventUID = eventUID;
        this.anotationType = anotationType;
        this.metadata = metadata;
        this.segments = segments;
    }

    public void generateUID() {
        annotationId = HashUID.builder().newId(new byte[0]);
    }

    public UID getAnnotationId() {
        return annotationId;
    }

    public String getEventDataType() {
        return eventDataType;
    }

    public String getEventShard() {
        return eventShard;
    }

    public UID getEventUID() {
        return eventUID;
    }

    public String getAnnotationType() {
        return anotationType;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public List<Segment> getSegments() {
        return segments;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String eventShard;
        private String eventDataType;
        private UID eventUID;
        private String annotationType;

        // what is this and how do we store it?
        private Map<String,String> metadata;

        private List<Segment> segments;

        public Builder setEventDataType(String eventDataType) {
            this.eventDataType = eventDataType;
            return this;
        }

        public Builder setEventShard(String eventShard) {
            this.eventShard = eventShard;
            return this;
        }

        public Builder setEventUID(UID eventUID) {
            this.eventUID = eventUID;
            return this;
        }

        public Builder setAnnotationType(String annotationType) {
            this.annotationType = annotationType;
            return this;
        }

        public Builder setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder putMetadata(String key, String value) {
            if (this.metadata == null) {
                this.metadata = new HashMap<>();
            }
            this.metadata.put(key, value);
            return this;
        }

        public Builder setSegments(List<Segment> segments) {
            this.segments = segments;
            return this;
        }

        public Builder addSegment(Segment segment) {
            if (this.segments == null) {
                this.segments = new ArrayList<>();
            }
            this.segments.add(segment);
            return this;
        }

        public Annotation build() {
            return new Annotation(eventShard, eventDataType, eventUID, annotationType, metadata, segments);
        }
    }
}
