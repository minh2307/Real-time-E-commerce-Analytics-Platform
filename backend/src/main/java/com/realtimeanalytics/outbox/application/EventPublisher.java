package com.realtimeanalytics.outbox.application;

import com.realtimeanalytics.outbox.domain.OutboxEvent;

public interface EventPublisher {
    void publish(OutboxEvent event) throws Exception;
}
