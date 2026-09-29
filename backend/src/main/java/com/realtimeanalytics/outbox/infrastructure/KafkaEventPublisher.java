package com.realtimeanalytics.outbox.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.realtimeanalytics.outbox.application.EventPublisher;
import com.realtimeanalytics.outbox.domain.OutboxEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.outbox.topic:ecommerce.events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Override
    public void publish(OutboxEvent event) throws Exception {
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("event_id", event.getId().toString());
        envelope.put("event_type", event.getEventType());
        envelope.put("aggregate_type", event.getAggregateType());
        envelope.put("aggregate_id", event.getAggregateId().toString());
        envelope.put("occurred_at", event.getCreatedAt().toString());
        
        // Parse payload as JSON and embed it
        if (event.getPayload() != null && !event.getPayload().isEmpty()) {
            JsonNode payloadNode = objectMapper.readTree(event.getPayload());
            envelope.set("payload", payloadNode);
        }

        String jsonMessage = objectMapper.writeValueAsString(envelope);
        
        // Use aggregate_id as the partition key to guarantee ordering per aggregate
        kafkaTemplate.send(topic, event.getAggregateId().toString(), jsonMessage).get();
    }
}
