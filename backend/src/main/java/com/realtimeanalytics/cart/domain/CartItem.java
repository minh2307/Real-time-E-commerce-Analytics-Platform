package com.realtimeanalytics.cart.domain;

import java.time.Instant;
import java.util.UUID;

public record CartItem(
    UUID id,
    UUID cartId,
    UUID productId,
    int quantity,
    Instant addedAt
) {
    public static CartItem create(UUID cartId, UUID productId, int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }
        return new CartItem(
            UUID.randomUUID(),
            cartId,
            productId,
            quantity,
            Instant.now()
        );
    }
}
