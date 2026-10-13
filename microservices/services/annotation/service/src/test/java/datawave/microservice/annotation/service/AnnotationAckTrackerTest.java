package datawave.microservice.annotation.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.integration.IntegrationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.core.context.SecurityContextHolder;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.util.lookup.service.LookupService;

class AnnotationAckTrackerTest {
    @Test
    void brokerConfirmationReleasesLatchWithoutRestAuthentication() {
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(IntegrationAutoConfiguration.class))
                        .withUserConfiguration(MethodSecurity.class, AnnotationAckTracker.class, AnnotationControllerV1.class, AnnotationServiceConfig.class,
                                        AnnotationSerializerConfiguration.class)
                        .withBean(AccumuloConnectionFactory.class, () -> mock(AccumuloConnectionFactory.class))
                        .withBean(LookupService.class, () -> mock(LookupService.class)).run(context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(AopUtils.isAopProxy(context.getBean(AnnotationControllerV1.class))).isTrue();
                            SecurityContextHolder.clearContext();
                            assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
                            CountDownLatch latch = new CountDownLatch(1);
                            AnnotationAckTracker tracker = context.getBean(AnnotationAckTracker.class);
                            tracker.putIfAbsent("broker-confirm", latch);
                            MessageChannel channel = context.getBean(AnnotationAckTracker.ANNOTATION_ACK_CHANNEL, MessageChannel.class);
                            assertThat(channel.send(new GenericMessage<>(new byte[] {1, 2}, Map.of(IntegrationMessageHeaderAccessor.CORRELATION_ID,
                                            "broker-confirm", MessageHeaders.CONTENT_TYPE, "application/x-protobuf", "amqp_publishConfirm", true)))).isTrue();
                            assertThat(latch.getCount()).isZero();
                            assertThat(tracker.remove("broker-confirm", latch)).isTrue();
                        });
    }

    @Configuration
    @EnableGlobalMethodSecurity(securedEnabled = true)
    static class MethodSecurity {}
}
