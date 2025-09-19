package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.data.AccumuloAnnotationUtil;
import datawave.annotation.data.AnnotationReadException;
import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.AnnotationUpdateException;
import datawave.annotation.data.AnnotationWriteException;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;

/** Used to read and write annotation data to Accumulo */
public class AnnotationDataAccess {

    public static final char NULL = '\0';
    public static final char MAX = '\uFFFF';

    protected static final Logger log = LoggerFactory.getLogger(AnnotationDataAccess.class);

    final AccumuloClient accumuloClient;
    final Authorizations authorizations;
    final String tableName;
    final AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> annotationSerializer;

    /**
     * Create the annotation data access object
     *
     * @param accumuloClient
     *            the accumulo client to use
     * @param authorizations
     *            the authorizations used for these operations
     * @param tableName
     *            the accumulo table to use
     * @param annotationSerializer
     *            the serializer to transform protobuf to accumulo entries
     */
    public AnnotationDataAccess(AccumuloClient accumuloClient, Authorizations authorizations, String tableName,
                    AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> annotationSerializer) {
        this.accumuloClient = accumuloClient;
        this.authorizations = authorizations;
        this.tableName = tableName;
        this.annotationSerializer = annotationSerializer;
    }

    /**
     * Get a specific annotation by id
     *
     * @param shard
     *            the shard for the document related to this annotation.
     * @param datatype
     *            the datatype of the document related to this annotation.
     * @param uid
     *            the document id related to this annotation
     * @param annotationType
     *            the type of annotation we're seeing.
     * @param annotationUid
     *            the annotation id of the annotation we want to fetch.
     * @return an Optional that will contain the retrieved annotation if found.
     */
    public Optional<Annotation> get(String shard, String datatype, String uid, String annotationType, String annotationUid) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL + annotationType;
            final String columnQualifierPrefix = annotationUid + NULL;
            final String columnQualifierRegex = columnQualifierPrefix + ".*";

            final Key startKey = new Key(shard, columnFamily, columnQualifierPrefix + NULL);
            final Key endKey = new Key(shard, columnFamily, columnQualifierPrefix + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#get", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamily, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();

            Annotation a = annotationSerializer.deserialize(it);
            return a == null ? Optional.empty() : Optional.of(a);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation for document: " + shard + "/" + datatype + "/" + uid
                            + " annotationType: " + annotationType + " annotationUid: " + annotationUid, e);
        }
    }

    /**
     * Get the annotation types for a document
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @return a collection of annotation types we have for this document.
     */
    public Collection<String> getTypes(String shard, String datatype, String uid) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamilyPrefix = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamilyPrefix + ".*";

            final Key startKey = new Key(shard, columnFamilyPrefix + NULL);
            final Key endKey = new Key(shard, columnFamilyPrefix + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAll", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return extractTypes(it);
        } catch (TableNotFoundException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation types for document: " + shard + "/" + datatype + "/" + uid,
                            e);
        }
    }

    /**
     * Get all annotations for a document
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @return a collection of annotation types we have for this document.
     */
    public List<Annotation> getAll(String shard, String datatype, String uid) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamilyPrefix = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamilyPrefix + ".*";

            final Key startKey = new Key(shard, columnFamilyPrefix + NULL);
            final Key endKey = new Key(shard, columnFamilyPrefix + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAll", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return processAnnotationsIterator(it);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading all annotations for document: " + shard + "/" + datatype + "/" + uid, e);
        }
    }

    /**
     * Get all annotations of a specific type for a document
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @param annotationType
     *            the annotation types we're interested
     * @return a list of annotations for this document of the specified type.
     */
    public List<Annotation> getAllForType(String shard, String datatype, String uid, String annotationType) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL + annotationType;

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAllForType", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamily, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return processAnnotationsIterator(it);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading " + annotationType + " type annotations for document: " + shard + "/"
                            + datatype + "/" + uid, e);
        }
    }

    /**
     * Get the annotation with the specified annotationId
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @param annotationId
     *            the id of the annotation we want to retrieve
     * @return an Optional that will contain the retrieved annotation if found.
     */
    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationId) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamily + ".*";
            final String columnQualifierRegex = annotationId + NULL + ".*";

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAnnotation", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            List<Annotation> annotations = processAnnotationsIterator(it);
            if (annotations.isEmpty()) {
                return Optional.empty();
            } else if (annotations.size() > 1) {
                // TODO: there should be only one here. what kind of exception should we throw?
            }

            return Optional.of(annotations.get(0));
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(
                            e.getClass().getSimpleName() + " reading annotationId " + annotationId + " for document: " + shard + "/" + datatype + "/" + uid, e);
        }
    }

    /**
     * Save a new annotation.
     *
     * @param annotation
     *            the annotation to save.
     * @return an Optional containing the save annotation if the save was successful.
     */
    public Optional<Annotation> save(Annotation annotation) {

        // TODO: validate the annotation
        // TODO: check for conflicts here, an annotation with an id that already exists should be rejected.
        // TODO: assign an id?

        try (BatchWriter writer = accumuloClient.createBatchWriter(tableName)) {
            Iterator<Map.Entry<Key,Value>> it = annotationSerializer.serialize(annotation);
            Mutation m = AccumuloAnnotationUtil.mutationAdapter(it, false);
            writer.addMutation(m);
            return Optional.of(annotation);
        } catch (TableNotFoundException | MutationsRejectedException | AnnotationSerializationException e) {
            throw new AnnotationWriteException(e.getClass().getSimpleName() + " saving annotation " + annotation, e);
        }
    }

    /**
     * Update an annotation.
     *
     * @param annotation
     *            the annotation to save.
     * @return an Optional containing the updated annotation if the update was successful.
     */
    public Optional<Annotation> update(Annotation annotation) {

        // TODO: validate the annotation

        String shard = annotation.getShard();
        String datatype = annotation.getDataType();
        String uid = annotation.getUid();
        String annotationId = annotation.getAnnotationId();

        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamily + ".*";
            final String columnQualifierRegex = annotationId + NULL + ".*";

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAnnotation", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator(); // these contain the entries for which we need to generate delete mutations.
            List<Annotation> annotations = processAnnotationsIterator(it);
            if (annotations.isEmpty()) {
                throw new AnnotationUpdateException("Unable to find annotation to update for document: " + shard + "/" + datatype + "/" + uid
                                + " and annotation id: " + annotationId);
            } else if (annotations.size() > 1) {
                throw new AnnotationUpdateException("Can't choose annotation to update, multiple annotations are present for document: " + shard + "/"
                                + datatype + "/" + uid + " and annotation id: " + annotationId);
            }

            // TODO: implement update logic here.

            return Optional.of(annotation);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationUpdateException(e.getClass().getSimpleName() + " when updating annotationId " + annotationId + " for document: " + shard + "/"
                            + datatype + "/" + uid, e);
        }
    }

    /**
     * Deletes all entries for annotation that matches the provided criteria
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @param annotationId
     *            the id of the annotation we want to delete.
     */
    public void delete(String shard, String datatype, String uid, String annotationId) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations); BatchWriter writer = accumuloClient.createBatchWriter(tableName)) {
            final String columnFamily = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamily + ".*";
            final String columnQualifierRegex = annotationId + NULL + ".*";

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAnnotation", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator(); // these contain the entries for which we need to generate delete mutations.
            Mutation m = AccumuloAnnotationUtil.mutationAdapter(it, true);
            writer.addMutation(m);
        } catch (TableNotFoundException | AnnotationSerializationException | MutationsRejectedException e) {
            throw new AnnotationUpdateException(
                            e.getClass().getSimpleName() + " deleting annotationId " + annotationId + " for document: " + shard + "/" + datatype + "/" + uid,
                            e);
        }
    }

    /**
     * Add a segment to an existing annotation.
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @param annotationId
     *            the id of the annotation to which we want to add the segment.
     * @param segment
     *            the segment to add.
     */
    public void addSegment(String shard, String datatype, String uid, String annotationId, Segment segment) {
        // TODO: implement me
    }

    /**
     * Update a segment in an existing annotation.
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @param annotationId
     *            the id of the annotation to which we want to add the segment.
     * @param segment
     *            the segment to update.
     */
    public void updateSegment(String shard, String datatype, String uid, String annotationId, Segment segment) {
        // TODO: implement me
    }

    /**
     * Deletes any segments with the id specified in an existing annotation.
     *
     * @param shard
     *            the shard for the document.
     * @param datatype
     *            the datatype of the document.
     * @param uid
     *            the document id.
     * @param annotationId
     *            the id of the annotation to which we want to add the segment.
     * @param segmentId
     *            the id of the segment to delete.
     */
    public void deleteSegment(String shard, String datatype, String uid, String annotationId, String segmentId) {
        // TODO: implement me
    }

    /**
     * The annotation type is always stored in the last slot of the column family, extract all the types found in the iterator to a set.
     *
     * @param it
     *            the iterator to process for input.
     * @return a list of distinct annotation types.
     */
    public Collection<String> extractTypes(Iterator<Map.Entry<Key,Value>> it) {
        final Set<String> annotationTypes = new TreeSet<>();
        while (it.hasNext()) {
            Map.Entry<Key,Value> e = it.next();
            Key k = e.getKey();
            String cf = k.getColumnFamily().toString();
            int typeSep = cf.lastIndexOf(NULL);
            annotationTypes.add(cf.substring(typeSep + 1));
        }
        return annotationTypes;
    }

    /**
     * Extract the data referenced by the interator into a collection of Annotation objects.
     *
     * @param it
     *            the iterator to process for input
     * @return a list of zero to many Annotations extracted from the iterator. Never null.
     * @throws AnnotationSerializationException
     *             if there is a problem deserializing any portions of the Accumulo entries.
     */
    public List<Annotation> processAnnotationsIterator(Iterator<Map.Entry<Key,Value>> it) throws AnnotationSerializationException {
        final List<Map.Entry<Key,Value>> buffer = new ArrayList<>();
        final List<Annotation> results = new ArrayList<>();
        String currentAnnotationId = null;

        // TODO: add visibility field to Annotation metadata?
        // TODO: interpret ColumnVisibilities to set access controls metadata?

        while (it.hasNext()) {
            Map.Entry<Key,Value> e = it.next();
            Key k = e.getKey();
            String cq = k.getColumnQualifier().toString();
            int idSep = cq.indexOf(NULL);
            String annotationId = cq.substring(0, idSep);
            if (currentAnnotationId == null) {
                // new iterator of keys to process
                currentAnnotationId = annotationId;
                buffer.add(e);
            } else if (currentAnnotationId.equals(annotationId)) {
                // still collecting keys for the current annotation.
                buffer.add(e);
            } else {
                // we've found new annotation, process the buffer,
                // clear it and add the current entry to the cleared empty buffer.
                Annotation a = annotationSerializer.deserialize(buffer.iterator());
                results.add(a);
                buffer.clear();

                currentAnnotationId = annotationId;
                buffer.add(e);

            }
        }

        if (!buffer.isEmpty()) {
            Annotation a = annotationSerializer.deserialize(buffer.iterator());
            results.add(a);
            buffer.clear();
        }

        return results;
    }
}
