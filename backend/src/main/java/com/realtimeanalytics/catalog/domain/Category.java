package com.realtimeanalytics.catalog.domain;

import java.time.Instant;
import java.util.UUID;

public record Category(
    UUID id,
    UUID parentId,
    String slug,
    String name,
    String description,
    Instant createdAt,
    Instant updatedAt
) {
    public static Category create(UUID parentId, String slug, String name, String description) {
        return new Category(
            UUID.randomUUID(),
            parentId,
            slug,
            name,
            description,
            Instant.now(),
            Instant.now()
        );
    }
}
