package com.realtimeanalytics.outbox.application;

import com.realtimeanalytics.outbox.domain.OutboxEvent;
import com.realtimeanalytics.outbox.domain.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutboxPollerTest {

    private OutboxEventRepository repository;
    private EventPublisher publisher;
    private OutboxPoller poller;

    @BeforeEach
    void setUp() {
        repository = Mockito.mock(OutboxEventRepository.class);
        publisher = Mockito.mock(EventPublisher.class);
        poller = new OutboxPoller(repository, publisher);
    }

    @Test
    void shouldPublishAndMarkAsPublished() throws Exception {
        OutboxEvent event = new OutboxEvent(UUID.randomUUID(), "ORDER", UUID.randomUUID(), "ORDER_CREATED", "1.0", "{}", OutboxEvent.Status.PENDING, Instant.now());
        when(repository.findPendingEvents(anyInt())).thenReturn(Collections.singletonList(event));

        poller.processOutboxEvents();

        verify(publisher).publish(event);
        assertEquals(OutboxEvent.Status.PUBLISHED, event.getStatus());
        assertNotNull(event.getPublishedAt());
        verify(repository).updateStatus(event);
    }

    @Test
    void shouldHandlePublishFailureAndIncrementRetry() throws Exception {
        OutboxEvent event = new OutboxEvent(UUID.randomUUID(), "ORDER", UUID.randomUUID(), "ORDER_CREATED", "1.0", "{}", OutboxEvent.Status.PENDING, Instant.now());
        when(repository.findPendingEvents(anyInt())).thenReturn(Collections.singletonList(event));
        
        doThrow(new RuntimeException("Kafka error")).when(publisher).publish(event);

        poller.processOutboxEvents();

        assertEquals(OutboxEvent.Status.PENDING, event.getStatus());
        assertEquals(1, event.getRetryCount());
        verify(repository).updateStatus(event);
    }

    @Test
    void shouldMarkFailedWhenMaxRetriesExceeded() throws Exception {
        OutboxEvent event = new OutboxEvent(UUID.randomUUID(), "ORDER", UUID.randomUUID(), "ORDER_CREATED", "1.0", "{}", OutboxEvent.Status.PENDING, Instant.now());
        event.setRetryCount(2); // Assuming MAX_RETRIES is 3, 2 + 1 = 3
        when(repository.findPendingEvents(anyInt())).thenReturn(Collections.singletonList(event));
        
        doThrow(new RuntimeException("Kafka error")).when(publisher).publish(event);

        poller.processOutboxEvents();

        assertEquals(OutboxEvent.Status.FAILED, event.getStatus());
        assertEquals(3, event.getRetryCount());
        verify(repository).updateStatus(event);
    }
}
