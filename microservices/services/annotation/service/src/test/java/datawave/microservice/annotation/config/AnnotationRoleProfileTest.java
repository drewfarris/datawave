package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;

class AnnotationRoleProfileTest {
    @Test
    void apiProfileLoadsPublisherSettingsWithoutConsumers() throws IOException {
        StandardEnvironment environment = configuration("annotation-api");
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(AnnotationRole.from(environment)).isEqualTo(AnnotationRole.API);
        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("annotationSource;busConsumer");
        assertThat(environment.getProperty("annotation.writers.file.enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("annotation.annotation-ack-timeout-millis", Integer.class)).isEqualTo(500);
    }

    @Test
    void writerProfileLoadsConsumerSettingsWithoutPublisher() throws IOException {
        StandardEnvironment environment = configuration("annotation-writer");
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(AnnotationRole.from(environment)).isEqualTo(AnnotationRole.WRITER);
        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("accumuloAnnotationSink;busConsumer");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.accumuloAnnotationSink-in-0.consumer.concurrency", Integer.class)).isEqualTo(1);
        assertThat(environment.getProperty("annotation.enable-internal-id-lookup", Boolean.class)).isFalse();
    }

    @Test
    void laterDeploymentProfileCanSelectAFileOnlyWriter() throws IOException {
        StandardEnvironment environment = configuration("annotation-writer", "deployment-files");
        environment.getPropertySources().addFirst(new MapPropertySource("deployment-files", Map.of("annotation.writers.accumulo.enabled", false,
                        "annotation.writers.dump.enabled", true, "spring.cloud.stream.bindings.dumpAnnotationSink-in-0.consumer.concurrency", 3)));
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("dumpAnnotationSink;busConsumer");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.dumpAnnotationSink-in-0.consumer.concurrency", Integer.class)).isEqualTo(3);
    }

    @Test
    void bothRoleProfilesFailRegardlessOfPropertyPrecedence() throws IOException {
        StandardEnvironment environment = configuration("annotation-api", "annotation-writer");

        assertThatThrownBy(() -> AnnotationRole.from(environment)).isInstanceOf(IllegalStateException.class)
                        .hasMessage("Activate only one of annotation-api and annotation-writer");
    }

    @Test
    void missingRoleOverlayCannotSilentlyStartCombined() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles("annotation-api");

        assertThatThrownBy(() -> AnnotationRole.from(environment)).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("check the Config Server role overlay");
    }

    @Test
    void conflictingRoleOverrideFails() throws IOException {
        StandardEnvironment environment = configuration("annotation-writer");
        environment.getPropertySources().addFirst(new MapPropertySource("conflicting-override", Map.of("annotation.role", "api")));

        assertThatThrownBy(() -> AnnotationRole.from(environment)).isInstanceOf(IllegalStateException.class).hasMessageContaining("annotation.role must match");
    }

    @Test
    void serviceAndDockerRoleOverlaysHaveIdenticalSettings() throws IOException {
        Path service = Path.of(System.getProperty("basedir")).toAbsolutePath();
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        for (String profile : new String[] {"annotation-api", "annotation-writer"}) {
            String filename = "annotation-" + profile + ".yml";
            assertThat(loader.load(profile, new FileSystemResource(service.resolve("src/main/config").resolve(filename))).get(0).getSource()).isEqualTo(
                            loader.load(profile, new FileSystemResource(service.resolve("../../../../docker/config").resolve(filename))).get(0).getSource());
        }
    }

    private static StandardEnvironment configuration(String... profiles) throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles(profiles);
        Path config = Path.of(System.getProperty("basedir"), "src/main/config");
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        environment.getPropertySources().addFirst(loader.load("shared", new FileSystemResource(config.resolve("annotation.yml"))).get(0));
        for (String profile : profiles) {
            Path overlay = config.resolve("annotation-" + profile + ".yml");
            if (java.nio.file.Files.exists(overlay)) {
                environment.getPropertySources().addFirst(loader.load(profile, new FileSystemResource(overlay)).get(0));
            }
        }
        return environment;
    }
}
