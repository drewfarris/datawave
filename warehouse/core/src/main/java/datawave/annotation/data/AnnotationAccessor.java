package datawave.annotation.data;

import java.util.List;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloClient;

import datawave.annotation.model.Annotation;
import datawave.data.hash.UID;

public class AnnotationAccessor {

    AccumuloClient accumuloClient;
    String tableName;

    public AnnotationAccessor(AccumuloClient accumuloClient, String tableName) {
        this.accumuloClient = accumuloClient;
    }

    /** Get a specific annotation */
    public Optional<Annotation> get(String shard, String datatype, UID uid, UID annotationUid) {

    }

    /** Get all annotations for a document */
    public List<Annotation> getAll(String shard, String datatype, UID uid) {

    }

    /** Save an annotation */
    public void save(Annotation a) {

    }

    /** Update an annotation */
    public void update(Annotation a) {

    }

    public void delete(Annotation a) {

    }
}
