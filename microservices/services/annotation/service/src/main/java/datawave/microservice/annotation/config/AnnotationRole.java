package datawave.microservice.annotation.config;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/**
 * Selects the responsibilities of an annotation deployment. Existing deployments retain both responsibilities by default.
 */
public enum AnnotationRole {
    COMBINED, API, WRITER;

    public boolean includesApi() {
        return this != WRITER;
    }

    public boolean includesWriter() {
        return this != API;
    }

    public static AnnotationRole from(Environment environment) {
        boolean apiProfile = environment.acceptsProfiles(Profiles.of("annotation-api"));
        boolean writerProfile = environment.acceptsProfiles(Profiles.of("annotation-writer"));
        if (apiProfile && writerProfile) {
            throw new IllegalStateException("Activate only one of annotation-api and annotation-writer");
        }
        AnnotationRole role = Binder.get(environment).bind("annotation.role", AnnotationRole.class).orElse(COMBINED);
        if ((apiProfile && role != API) || (writerProfile && role != WRITER)) {
            throw new IllegalStateException("annotation.role must match the active annotation-api or annotation-writer profile; "
                            + "check the Config Server role overlay and remove conflicting role overrides");
        }
        return role;
    }
}
