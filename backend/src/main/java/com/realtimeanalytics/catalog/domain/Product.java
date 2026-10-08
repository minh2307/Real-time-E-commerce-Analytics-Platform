package com.realtimeanalytics.catalog.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Product(
    UUID id,
    UUID categoryId,
    String sku,
    String title,
    String description,
    BigDecimal price,
    Instant createdAt,
    Instant updatedAt
) {
    public static Product create(UUID categoryId, String sku, String title, String description, BigDecimal price) {
        if (price.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Price cannot be negative");
        }
        return new Product(
            UUID.randomUUID(),
            categoryId,
            sku,
            title,
            description,
            price,
            Instant.now(),
            Instant.now()
        );
    }
}
