package datawave.annotation.data;

import datawave.annotation.model.Annotation;

public interface AnnotationSerializer<T> {
    T serialize(Annotation annotation);
    Annotation deserialize(T input);
}
