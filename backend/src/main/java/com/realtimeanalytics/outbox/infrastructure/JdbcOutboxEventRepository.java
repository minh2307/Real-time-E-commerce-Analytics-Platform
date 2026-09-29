package com.realtimeanalytics.outbox.infrastructure;

import com.realtimeanalytics.outbox.domain.OutboxEvent;
import com.realtimeanalytics.outbox.domain.OutboxEventRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

@Repository
public class JdbcOutboxEventRepository implements OutboxEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcOutboxEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void save(OutboxEvent event) {
        String sql = "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, schema_version, payload, status, retry_count, created_at) " +
                     "VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)";
        jdbcTemplate.update(sql,
                event.getId(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getEventType(),
                event.getSchemaVersion(),
                event.getPayload(),
                event.getStatus().name(),
                event.getRetryCount(),
                Timestamp.from(event.getCreatedAt())
        );
    }

    @Override
    public List<OutboxEvent> findPendingEvents(int limit) {
        String sql = "SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT ? FOR UPDATE SKIP LOCKED";
        return jdbcTemplate.query(sql, new OutboxEventRowMapper(), limit);
    }

    @Override
    public void updateStatus(OutboxEvent event) {
        String sql = "UPDATE outbox_events SET status = ?, retry_count = ?, published_at = ?, last_error = ? WHERE id = ?";
        jdbcTemplate.update(sql,
                event.getStatus().name(),
                event.getRetryCount(),
                event.getPublishedAt() != null ? Timestamp.from(event.getPublishedAt()) : null,
                event.getLastError(),
                event.getId()
        );
    }

    private static class OutboxEventRowMapper implements RowMapper<OutboxEvent> {
        @Override
        public OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            OutboxEvent event = new OutboxEvent();
            event.setId(rs.getObject("id", UUID.class));
            event.setAggregateType(rs.getString("aggregate_type"));
            event.setAggregateId(rs.getObject("aggregate_id", UUID.class));
            event.setEventType(rs.getString("event_type"));
            event.setSchemaVersion(rs.getString("schema_version"));
            event.setPayload(rs.getString("payload"));
            event.setStatus(OutboxEvent.Status.valueOf(rs.getString("status")));
            event.setRetryCount(rs.getInt("retry_count"));
            event.setCreatedAt(rs.getTimestamp("created_at").toInstant());
            
            Timestamp publishedAt = rs.getTimestamp("published_at");
            if (publishedAt != null) {
                event.setPublishedAt(publishedAt.toInstant());
            }
            event.setLastError(rs.getString("last_error"));
            
            return event;
        }
    }
}
