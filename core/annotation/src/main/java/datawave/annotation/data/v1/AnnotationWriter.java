package datawave.annotation.data.v1;

import java.util.Optional;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;

/**
 * Write contract for annotation data stores.
 * <p>
 * Implementations are responsible for validating writable annotation data, assigning store-managed identifiers when needed, and persisting annotation and
 * annotation source changes.
 */
public interface AnnotationWriter {

    /**
     * Adds a new annotation source.
     *
     * @param annotationSource
     *            the annotation source to add; callers should not pre-populate store-managed ids
     * @return the persisted annotation source, including any ids assigned by the writer
     */
    Optional<AnnotationSource> addAnnotationSource(AnnotationSource annotationSource);

    /**
     * Adds a new annotation.
     *
     * @param annotation
     *            the annotation to add; callers should not pre-populate store-managed annotation or segment ids
     * @return the persisted annotation, including any ids assigned by the writer
     */
    Optional<Annotation> addAnnotation(Annotation annotation);

    /**
     * Creates an update for an existing annotation.
     * <p>
     * Implementations may preserve previous annotation versions and link the new annotation back to {@code targetAnnotationId} instead of overwriting the
     * existing annotation in place.
     *
     * @param targetAnnotationId
     *            the id of the existing annotation being updated
     * @param annotation
     *            the updated annotation data to persist
     * @return the persisted update annotation, including any ids assigned by the writer
     */
    Optional<Annotation> updateAnnotation(String targetAnnotationId, Annotation annotation);

    /**
     * Deletes all stored entries for a document annotation id.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id to delete
     */
    void delete(String shard, String datatype, String uid, String annotationId);

    /**
     * Adds a segment to an existing annotation.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id that should receive the segment
     * @param segment
     *            the segment to add
     */
    void addSegment(String shard, String datatype, String uid, String annotationId, Segment segment);

    /**
     * Updates a segment in an existing annotation.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id containing the segment
     * @param segment
     *            the replacement segment data
     */
    void updateSegment(String shard, String datatype, String uid, String annotationId, Segment segment);

    /**
     * Deletes a segment from an existing annotation.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id containing the segment
     * @param segmentId
     *            the segment id to delete
     */
    void deleteSegment(String shard, String datatype, String uid, String annotationId, String segmentId);
}
