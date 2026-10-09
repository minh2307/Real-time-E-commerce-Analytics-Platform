package com.realtimeanalytics.cart.domain;

import java.time.Instant;
import java.util.UUID;

public record Cart(
    UUID id,
    UUID userId,
    Instant createdAt,
    Instant updatedAt
) {
    public static Cart create(UUID userId) {
        return new Cart(
            UUID.randomUUID(),
            userId,
            Instant.now(),
            Instant.now()
        );
    }
}
