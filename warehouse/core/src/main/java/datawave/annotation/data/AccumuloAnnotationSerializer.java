package datawave.annotation.data;

import datawave.annotation.model.Annotation;
import datawave.annotation.model.Segment;
import datawave.annotation.protobuf.SegmentData;
import datawave.data.hash.UID;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AccumuloAnnotationSerializer implements AnnotationSerializer<List<Map.Entry<Key, Value>>> {
    public static final char NULL = 0x0;
    public static final Value EMPTY = new Value();

    @Override
    public List<Map.Entry<Key, Value>> serialize(Annotation annotation) throws IllegalArgumentException {
        //TODO: validate that the annotation has one or more segments
        //TODO: validate that the annotation has a valid shard (at least?)

        Key baseKey = generateBaseKey(annotation);

        List<Map.Entry<Key, Value>> serializedResults = new ArrayList<>();
        // TODO: convert the annotation id to a string/byte array once and pass this down instead of converting it each
        // time
        if (annotation.getMetadata() != null && !annotation.getMetadata().isEmpty()) {
            serializeMetadata(baseKey, annotation.getAnnotationId(), annotation.getMetadata(), serializedResults);
        }

        serializeSegments(baseKey, annotation.getAnnotationId(), annotation.getSegments(), serializedResults);

        return serializedResults;
    }

    @Override
    public Annotation deserialize(List<Map.Entry<Key, Value>> keys) {
        //TODO: add convenience method that takes an iterator?
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("Accumulo key list can't be empty");
        }

        //TODO: validate that there is at least one row here - the AnnotationSegment information
        //TODO: validate that the keys all have the same row and column family.
        //TODO: validate that the keys all have the same annotationUID.
        //TODO: validate that the keys have metadata rows - with a 3 part CQ and empty value.
        //TODO: validate that the keys have segment rows - with a 2 part CQ and a valid SegmentData protobuf

        String eventShard = "";
        String eventDataType = "";
        UID eventUID = "";

        Annotation a = Annotation.newBuilder()
                .setEventShard(eventShard)
                .setEventDataType(eventDataType)
                .setEventUID(eventUID)
                .setMetadata(Collections.emptyMap())
                .setSegments(Collections.emptyList())
                .build()
    }


    /** Generate the base key that will be used for serialization throughout this class */
    public static Key generateBaseKey(Annotation annotation) {
        String rowId = annotation.getEventShard();
        String columnFamily = annotation.getEventDataType() + NULL + annotation.getEventUID() + NULL + annotation.getType();

        //TODO: add timestamp and visibility
        return Key.builder().row(rowId).family(columnFamily).build();
    }

    /** Serialize an Annotation's metdata map to a series of Accumulo key, value pairs written to the list provided
     *
     * @param baseKey the base key for the annotation.
     * @param annotationId the annotation id we are serializing
     * @param metadata the metadata map to serialize
     * @param serializedResults serialized pairs will be written to a provided list.
     */
    public static void serializeMetadata(Key baseKey, UID annotationId, Map<String, String> metadata, List<Map.Entry<Key, Value>> serializedResults) {
        for (Map.Entry<String, String> entry: metadata.entrySet()) {
            serializedResults.add(serializeMetadata(baseKey, annotationId, entry.getKey(), entry.getValue()));
        }
    }

    /** Serialize a single map entry to an Accumulo key, value pair.
     *
     * @param baseKey
     * @param annotationId
     * @param metadataKey
     * @param metadataValue
     * @return
     */
    public static Map.Entry<Key, Value> serializeMetadata(Key baseKey, UID annotationId, String metadataKey, String metadataValue) {
        final String columnQualifier = "" + annotationId + NULL + metadataKey + NULL + metadataValue;
        final Key key = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(baseKey.getColumnFamilyData().getBackingArray())
                .qualifier(columnQualifier)
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();

        return Map.entry(key, EMPTY);
    }

    public static void serializeSegments(Key baseKey, UID annotationId, List<Segment> segments, List<Map.Entry<Key, Value>> serializedResults) {
        for (Segment segment: segments) {
            serializedResults.add(serializeSegment(baseKey, annotationId, segment));
        }
    }

    public static Map.Entry<Key, Value> serializeSegment(Key baseKey, UID annotationId, Segment segment) {
        SegmentData segmentData = segment.getSegmentData();
        Value value = new Value(segmentData.toByteArray());

        final String columnQualifier = "" + annotationId + NULL + segment.getSegmentId();
        final Key key = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(baseKey.getColumnFamilyData().getBackingArray())
                .qualifier(columnQualifier)
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();

        return Map.entry(key, value);
    }
}
