package datawave.microservice.annotation.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import datawave.microservice.annotation.config.AnnotationRole;
import datawave.microservice.annotation.config.ConditionalOnAnnotationRole;
import lombok.extern.slf4j.Slf4j;

/**
 * Tracks in-flight annotation write operations by correlation ID so that async ack/timeout callbacks can resolve the original request.
 */
@Component
@ConditionalOnAnnotationRole({AnnotationRole.API, AnnotationRole.COMBINED})
@Slf4j
public class AnnotationAckTracker {
    public static final String ANNOTATION_ACK_CHANNEL = "annotationAckChannel";

    private final Map<String,CountDownLatch> correlationLatchMap = new ConcurrentHashMap<>();

    /**
     * Broker callbacks run without a REST user's security context. Keep this endpoint outside the secured controller.
     *
     * @param message
     *            the producer confirmation, including the original correlation ID
     */
    @ServiceActivator(inputChannel = ANNOTATION_ACK_CHANNEL)
    public void processConfirmAck(Message<?> message) {
        Object correlationId = message.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
        if (correlationId == null) {
            log.warn("No correlation ID found in confirm ack message");
        } else if (!countdown(correlationId.toString())) {
            log.warn("Unable to decrement latch for annotation ID [{}]", correlationId);
        }
    }

    /**
     * Register a new latch for the given correlation ID. If a latch already exists for this ID, the existing latch is returned instead.
     *
     * @param correlationId
     *            the correlation ID to register
     * @param latch
     *            the latch to register
     * @return the previously-registered latch for this ID, or {@code null} if none existed
     */
    public CountDownLatch putIfAbsent(String correlationId, CountDownLatch latch) {
        return correlationLatchMap.putIfAbsent(correlationId, latch);
    }

    /**
     * Remove and return the latch for the given correlation ID.
     *
     * @param correlationId
     *            the correlation ID whose latch should be removed
     * @param latch
     *            the expected latch instance
     * @return {@code true} if the latch was removed, {@code false} otherwise
     */
    public boolean remove(String correlationId, CountDownLatch latch) {
        return correlationLatchMap.remove(correlationId, latch);
    }

    /**
     * Count down the latch associated with the given correlation ID, if one exists. Uses {@link ConcurrentHashMap#computeIfPresent} to atomically fetch and
     * count down the latch, eliminating the {@code containsKey()} → {@code get()} race that could occur if another thread removes the latch between the check
     * and the count-down.
     *
     * @param correlationId
     *            the correlation ID whose latch should be counted down
     * @return {@code true} if a latch was found and counted down, {@code false} otherwise
     */
    public boolean countdown(String correlationId) {
        return correlationLatchMap.computeIfPresent(correlationId, (key, latch) -> {
            latch.countDown();
            return latch;
        }) != null;
    }

    /**
     * Check whether a latch exists for the given correlation ID.
     *
     * @param correlationId
     *            the correlation ID to check
     * @return {@code true} if a latch exists, {@code false} otherwise
     */
    public boolean contains(String correlationId) {
        return correlationLatchMap.containsKey(correlationId);
    }

    /**
     * Check whether there are no tracked latches.
     *
     * @return {@code true} if no latches are currently tracked
     */
    public boolean isEmpty() {
        return correlationLatchMap.isEmpty();
    }

    /**
     * Clear all tracked latches. Used between tests to prevent leakage.
     */
    public void clear() {
        correlationLatchMap.clear();
    }
}
