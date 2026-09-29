package com.realtimeanalytics.outbox.domain;

import java.util.List;

public interface OutboxEventRepository {
    void save(OutboxEvent event);
    List<OutboxEvent> findPendingEvents(int limit);
    void updateStatus(OutboxEvent event);
}
