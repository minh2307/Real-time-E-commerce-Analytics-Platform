package com.realtimeanalytics.outbox.application;

import com.realtimeanalytics.outbox.domain.OutboxEvent;
import com.realtimeanalytics.outbox.domain.OutboxEventRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class OutboxService {
    
    private final OutboxEventRepository repository;
    
    public OutboxService(OutboxEventRepository repository) {
        this.repository = repository;
    }
    
    public void saveEvent(String aggregateType, UUID aggregateId, String eventType, String payload) {
        OutboxEvent event = new OutboxEvent(
            UUID.randomUUID(),
            aggregateType,
            aggregateId,
            eventType,
            "1.0",
            payload,
            OutboxEvent.Status.PENDING,
            Instant.now()
        );
        repository.save(event);
    }
}
