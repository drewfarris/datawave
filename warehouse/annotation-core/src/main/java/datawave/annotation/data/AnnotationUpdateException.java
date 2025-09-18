package datawave.annotation.data;

public class AnnotationUpdateException extends RuntimeException {

    private static final long serialVersionUID = 4091754162164238510L;

    public AnnotationUpdateException(String message) {
        super(message);
    }

    public AnnotationUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

}
