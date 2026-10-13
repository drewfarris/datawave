package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.config.AccumuloConfiguration;
import datawave.microservice.annotation.common.config.AnnotationJsonConfiguration;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.service.AnnotationAckTracker;
import datawave.microservice.annotation.service.AnnotationControllerV1;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.util.lookup.common.LookupRequest;
import datawave.microservice.annotation.util.lookup.config.HttpClientConfig;
import datawave.microservice.annotation.util.lookup.config.LookupConfig;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;
import datawave.microservice.annotation.writers.file.config.DumpAnnotationWriterConfig;
import datawave.microservice.annotation.writers.file.config.FileAnnotationWriterConfig;
import datawave.microservice.annotation.writers.log.config.LogAnnotationWriterProperties;

class AnnotationRoleConfigurationTest {
    @TempDir
    java.nio.file.Path files;

    private final ApplicationContextRunner roles = new ApplicationContextRunner().withUserConfiguration(RoleComponents.class);

    @Test
    void apiRoleDoesNotInitializeAnyQueueWriterEvenWhenSharedConfigEnablesThem() {
        roles.withUserConfiguration(ApiDependencies.class).withPropertyValues("annotation.role=api", "annotation.writers.accumulo.enabled=true",
                        "annotation.writers.log.enabled=true", "annotation.writers.dump.enabled=true", "annotation.writers.file.enabled=false",
                        "spring.cloud.function.definition=annotationSource;logAnnotationSink;accumuloAnnotationSink;dumpAnnotationSink;busConsumer")
                        .run(context -> {
                            assertThat(context).hasNotFailed().hasSingleBean(AnnotationControllerV1.class).hasSingleBean(AnnotationAckTracker.class)
                                            .hasSingleBean(LookupService.class).hasSingleBean(ExecutorService.class).hasBean("annotationSource")
                                            .doesNotHaveBean("accumuloAnnotationWriter").doesNotHaveBean("accumuloAnnotationSink")
                                            .doesNotHaveBean("logAnnotationSink").doesNotHaveBean("dumpAnnotationSink").doesNotHaveBean("fileAnnotationWriter")
                                            .doesNotHaveBean("accumuloHealthChecker");
                            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                                            .isEqualTo("annotationSource;busConsumer");
                            Mockito.verifyNoInteractions(context.getBean(AccumuloConnectionFactory.class));
                        });
    }

    @Test
    void writerRoleStartsWithoutApiDependenciesOrDirectFileFallback() {
        roles.withUserConfiguration(AccumuloConfiguration.class).withPropertyValues("annotation.role=writer", "annotation.writers.log.enabled=true",
                        "annotation.writers.file.enabled=true", "annotation.writers.file.pathUri=invalid://must-not-be-created",
                        "spring.cloud.function.definition=annotationSource;logAnnotationSink;accumuloAnnotationSink;dumpAnnotationSink;busConsumer")
                        .run(context -> {
                            assertThat(context).hasNotFailed().hasBean("logAnnotationWriter").hasBean("logAnnotationSink").hasBean("jacksonAnnotationModule")
                                            .doesNotHaveBean(AnnotationControllerV1.class).doesNotHaveBean(AnnotationAckTracker.class)
                                            .doesNotHaveBean(LookupService.class).doesNotHaveBean(LookupRequest.class).doesNotHaveBean("httpClient")
                                            .doesNotHaveBean("lookupProperties").doesNotHaveBean("annotationSource")
                                            .doesNotHaveBean("federatedReadExecutorService").doesNotHaveBean("fileAnnotationWriter")
                                            .doesNotHaveBean(AccumuloConnectionFactory.class).doesNotHaveBean("connectionPoolsProperties");
                            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                                            .isEqualTo("logAnnotationSink;busConsumer");
                        });
    }

    @Test
    void defaultRoleRetainsApiAndConfiguredConsumersAndFileFallback() {
        roles.withUserConfiguration(ApiDependencies.class).withPropertyValues("annotation.writers.log.enabled=true", "annotation.writers.file.enabled=true",
                        "annotation.writers.file.pathUri=" + files.toUri()).run(context -> {
                            assertThat(context).hasNotFailed().hasSingleBean(AnnotationControllerV1.class).hasBean("annotationSource")
                                            .hasBean("logAnnotationSink").hasBean("fileAnnotationWriter");
                            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                                            .isEqualTo("annotationSource;logAnnotationSink;busConsumer");
                        });
    }

    @Test
    void dumpConsumerProvidesQueueDrivenFileOutputInWriterRole() {
        roles.withPropertyValues("annotation.role=writer", "annotation.writers.dump.enabled=true", "annotation.writers.dump.pathUri=" + files.toUri(),
                        "spring.cloud.bus.enabled=false").run(context -> {
                            assertThat(context).hasNotFailed().hasBean("dumpAnnotationSink").hasBean("dumpAnnotationWriter").doesNotHaveBean("annotationSource")
                                            .doesNotHaveBean("fileAnnotationWriter");
                            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition")).isEqualTo("dumpAnnotationSink");
                        });
    }

    @Test
    void invalidRoleFailsRatherThanSilentlyStartingCombined() {
        roles.withPropertyValues("annotation.role=wrtier").run(context -> assertThat(context).hasFailed());
    }

    @Test
    void writerWithoutEnabledQueueConsumerFailsClearly() {
        roles.withPropertyValues("annotation.role=writer", "annotation.writers.file.enabled=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessage("annotation.role=writer requires an enabled and selected log, accumulo, or dump queue consumer");
        });
    }

    @Test
    void explicitStreamDefinitionTakesPrecedenceAndPreservesUnrelatedFunctions() {
        roles.withPropertyValues("annotation.role=writer", "annotation.writers.log.enabled=true", "spring.cloud.bus.enabled=false",
                        "spring.cloud.function.definition=annotationSource",
                        "spring.cloud.stream.function.definition=annotationSource|other;logAnnotationSink;custom|other;busConsumer").run(context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(context.getEnvironment().getProperty("spring.cloud.function.definition")).isEqualTo("logAnnotationSink;custom|other");
                            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.autodetect", Boolean.class)).isFalse();
                        });
    }

    @Test
    void excludedConsumerInExplicitDefinitionCannotProduceAnIdleWriterDeployment() {
        roles.withPropertyValues("annotation.role=writer", "annotation.writers.log.enabled=true",
                        "spring.cloud.function.definition=annotationSource;busConsumer").run(context -> assertThat(context).hasFailed());
    }

    @Test
    void sharedConfigServerFunctionsRespectDeploymentEnvironmentRole() {
        roles.withInitializer(context -> {
            context.getEnvironment().getPropertySources().addLast(new MapPropertySource("packaged-default", java.util.Map.of("annotation.role", "combined")));
            context.getEnvironment().getPropertySources()
                            .addFirst(new SystemEnvironmentPropertySource("deployment-systemEnvironment", java.util.Map.of("ANNOTATION_ROLE", "writer")));
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("remote-config", java.util.Map.of("spring.cloud.function.definition",
                            "annotationSource;logAnnotationSink;accumuloAnnotationSink;dumpAnnotationSink;busConsumer")));
        }).withPropertyValues("annotation.writers.log.enabled=true").run(context -> {
            assertThat(context).hasNotFailed().hasBean("logAnnotationSink").doesNotHaveBean("annotationSource");
        });
    }

    @Configuration(proxyBeanMethods = false)
    @Import({AnnotationRoleConfiguration.class, AnnotationServiceConfig.class, AnnotationJsonConfiguration.class, AnnotationSerializerConfiguration.class,
            AnnotationControllerV1.class, AnnotationAckTracker.class, LookupConfig.class, HttpClientConfig.class, LookupRequest.class, LookupService.class,
            LogAnnotationWriterProperties.class, AccumuloAnnotationWriterConfig.class, FileAnnotationWriterConfig.class, DumpAnnotationWriterConfig.class})
    static class RoleComponents {}

    @Configuration(proxyBeanMethods = false)
    static class ApiDependencies {
        @Bean
        AccumuloConnectionFactory connectionFactory() {
            return Mockito.mock(AccumuloConnectionFactory.class);
        }

        @Bean
        SSLContext sslContext() throws Exception {
            return SSLContext.getDefault();
        }
    }
}
