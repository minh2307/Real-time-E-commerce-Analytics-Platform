package com.realtimeanalytics.inventory.domain;

import java.time.Instant;
import java.util.UUID;

public record Inventory(
    UUID productId,
    int availableQuantity,
    int reservedQuantity,
    long version,
    Instant updatedAt
) {
    public static Inventory create(UUID productId, int availableQuantity) {
        if (availableQuantity < 0) {
            throw new IllegalArgumentException("Available quantity cannot be negative");
        }
        return new Inventory(
            productId,
            availableQuantity,
            0,
            0L,
            Instant.now()
        );
    }
}
