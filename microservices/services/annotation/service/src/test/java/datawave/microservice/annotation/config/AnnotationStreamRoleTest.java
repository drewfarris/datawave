package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.integration.IntegrationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.function.FunctionConfiguration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.ClassUtils;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationJsonConfiguration;
import datawave.microservice.annotation.messaging.AnnotationMessagingConfiguration;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.writers.log.config.LogAnnotationWriterProperties;

/**
 * Initializes the real Spring Cloud function catalog and stream binding lifecycle with a recording binder instead of an external RabbitMQ broker.
 */
class AnnotationStreamRoleTest {
    private ApplicationContextRunner streams(RecordingBinder binder) {
        BinderFactory factory = mock(BinderFactory.class);
        doReturn(binder).when(factory).getBinder(any(), any());
        return new ApplicationContextRunner()
                        .withConfiguration(AutoConfigurations.of(IntegrationAutoConfiguration.class, ContextFunctionCatalogAutoConfiguration.class,
                                        ClassUtils.resolveClassName("org.springframework.cloud.function.cloudevent.CloudEventsFunctionExtensionConfiguration",
                                                        getClass().getClassLoader()),
                                        BindingServiceConfiguration.class, FunctionConfiguration.class))
                        .withUserConfiguration(AnnotationRoleConfiguration.class, AnnotationServiceConfig.class, AnnotationJsonConfiguration.class,
                                        AnnotationMessagingConfiguration.class, LogAnnotationWriterProperties.class)
                        .withBean(BinderFactory.class, () -> factory).withPropertyValues("spring.cloud.bus.enabled=false",
                                        "annotation.writers.log.enabled=true",
                                        "spring.cloud.function.definition=annotationSource;logAnnotationSink;accumuloAnnotationSink;dumpAnnotationSink;busConsumer",
                                        "spring.cloud.stream.bindings.annotationSource-out-0.destination=annotation",
                                        "spring.cloud.stream.bindings.annotationSource-out-0.content-type=application/x-protobuf",
                                        "spring.cloud.stream.bindings.logAnnotationSink-in-0.destination=annotation",
                                        "spring.cloud.stream.bindings.logAnnotationSink-in-0.content-type=application/x-protobuf",
                                        "spring.cloud.stream.bindings.logAnnotationSink-in-0.group=log");
    }

    @Test
    void apiBindsOnlyTheProducer() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("annotation.role=api").run(context -> {
            assertThat(context).hasNotFailed().hasBean("annotationSource-out-0").doesNotHaveBean("logAnnotationSink-in-0");
            assertThat(binder.producers).isEqualTo(1);
            assertThat(binder.consumers).isZero();
        });
    }

    @Test
    void writerBindsOnlyTheSelectedConsumer() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("annotation.role=writer").run(context -> {
            assertThat(context).hasNotFailed().hasBean("logAnnotationSink-in-0").hasBean("jacksonAnnotationModule").doesNotHaveBean("annotationSource-out-0");
            assertThat(binder.producers).isZero();
            assertThat(binder.consumers).isEqualTo(1);
        });
    }

    @Test
    void defaultCombinedBindsBothResponsibilities() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).run(context -> {
            assertThat(context).hasNotFailed().hasBean("annotationSource-out-0").hasBean("logAnnotationSink-in-0");
            assertThat(binder.producers).isEqualTo(1);
            assertThat(binder.consumers).isEqualTo(1);
        });
    }

    @Test
    void streamBindingsSerializeAndConsumeProtobufBytes() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).run(context -> {
            assertThat(context).hasNotFailed().hasBean("annotationProtobufMessageConverter");
            AnnotationMessage expected = AnnotationMessage.newBuilder().setSource("annotation-role-conversion-test")
                            .addAnnotations(AnnotationTestDataUtil.generateTestAnnotation()).build();
            // Rabbit's binder publishes this event to start the reactive supplier.
            context.publishEvent(new BindingCreatedEvent(binder.producerBinding));
            assertThat(context.getBean(AnnotationSupplier.class).send(new GenericMessage<>(expected))).isTrue();

            Message<?> wire = binder.messages.poll(3, TimeUnit.SECONDS);
            assertThat(wire).isNotNull();
            assertThat(wire.getPayload()).isInstanceOf(byte[].class);
            assertThat(AnnotationMessage.parseFrom((byte[]) wire.getPayload())).isEqualTo(expected);
            // Deliver broker-shaped bytes through the real inbound conversion and function invocation chain.
            assertThat(binder.consumerChannel.send(new GenericMessage<>(wire.getPayload(), Map.of(MessageHeaders.CONTENT_TYPE, "application/x-protobuf"))))
                            .isTrue();
        });
    }

    static class RecordingBinder implements Binder<MessageChannel,ConsumerProperties,ProducerProperties> {
        int producers;
        int consumers;
        final BlockingQueue<Message<?>> messages = new LinkedBlockingQueue<>();
        MessageChannel consumerChannel;
        Binding<MessageChannel> producerBinding;

        @Override
        @SuppressWarnings("unchecked")
        public Binding<MessageChannel> bindConsumer(String destination, String group, MessageChannel channel, ConsumerProperties properties) {
            assertThat(destination).isEqualTo("annotation");
            assertThat(group).isEqualTo("log");
            consumers++;
            consumerChannel = channel;
            return mock(Binding.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Binding<MessageChannel> bindProducer(String destination, MessageChannel channel, ProducerProperties properties) {
            assertThat(destination).isEqualTo("annotation");
            producers++;
            ((SubscribableChannel) channel).subscribe(messages::add);
            producerBinding = mock(Binding.class);
            return producerBinding;
        }
    }
}
