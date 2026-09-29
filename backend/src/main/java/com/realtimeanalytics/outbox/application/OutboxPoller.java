package com.realtimeanalytics.outbox.application;

import com.realtimeanalytics.outbox.domain.OutboxEvent;
import com.realtimeanalytics.outbox.domain.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final int BATCH_SIZE = 50;
    private static final int MAX_RETRIES = 3;

    private final OutboxEventRepository repository;
    private final EventPublisher publisher;

    public OutboxPoller(OutboxEventRepository repository, EventPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll.interval:2000}")
    @Transactional
    public void processOutboxEvents() {
        List<OutboxEvent> events = repository.findPendingEvents(BATCH_SIZE);
        if (events.isEmpty()) {
            return;
        }

        for (OutboxEvent event : events) {
            try {
                publisher.publish(event);
                event.markPublished(Instant.now());
                repository.updateStatus(event);
            } catch (Exception e) {
                log.error("Failed to publish outbox event id: {}", event.getId(), e);
                event.setRetryCount(event.getRetryCount() + 1);
                if (event.getRetryCount() >= MAX_RETRIES) {
                    event.markFailed(e.getMessage());
                }
                repository.updateStatus(event);
            }
        }
    }
}
