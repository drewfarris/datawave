package datawave.annotation.data;

import datawave.annotation.model.Annotation;

public interface AnnotationSerializer<T> {
    T serialize(Annotation annotation) throws AnnotationSerializationException;

    Annotation deserialize(T input) throws AnnotationSerializationException;
}
