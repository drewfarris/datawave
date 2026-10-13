package datawave.microservice.annotation.common;

import java.util.function.Supplier;

import org.springframework.messaging.Message;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

public class AnnotationSupplier implements Supplier<Flux<Message<AnnotationMessage>>> {
    private final Sinks.Many<Message<AnnotationMessage>> messagingSink = Sinks.many().multicast().onBackpressureBuffer();

    // Reactor's sink rejects concurrent emissions. Serialize this brief operation so overlapping REST requests
    // are not mistaken for unavailable messaging infrastructure. Confirmation waits happen outside this lock.
    public synchronized boolean send(Message<AnnotationMessage> annotationMessage) {
        return messagingSink.tryEmitNext(annotationMessage).isSuccess();
    }

    @Override
    public Flux<Message<AnnotationMessage>> get() {
        return messagingSink.asFlux().subscribeOn(Schedulers.boundedElastic()).share();
    }
}
