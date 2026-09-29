package com.realtimeanalytics.outbox.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtimeanalytics.outbox.domain.OutboxEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventPublisherTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaEventPublisher publisher;
    private ObjectMapper objectMapper;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        objectMapper = new ObjectMapper();
        publisher = new KafkaEventPublisher(kafkaTemplate, objectMapper, "test.events");
    }

    @Test
    void shouldPublishEventWithCorrectEnvelope() throws Exception {
        UUID aggregateId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = new OutboxEvent(
                eventId,
                "ORDER",
                aggregateId,
                "ORDER_CREATED",
                "1.0",
                "{\"foo\":\"bar\"}",
                OutboxEvent.Status.PENDING,
                Instant.now()
        );

        when(kafkaTemplate.send(eq("test.events"), eq(aggregateId.toString()), Mockito.anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(event);

        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("test.events"), eq(aggregateId.toString()), messageCaptor.capture());

        String message = messageCaptor.getValue();
        assertTrue(message.contains("\"event_id\":\"" + eventId + "\""));
        assertTrue(message.contains("\"event_type\":\"ORDER_CREATED\""));
        assertTrue(message.contains("\"aggregate_type\":\"ORDER\""));
        assertTrue(message.contains("\"aggregate_id\":\"" + aggregateId + "\""));
        assertTrue(message.contains("\"payload\":{\"foo\":\"bar\"}"));
    }
}
