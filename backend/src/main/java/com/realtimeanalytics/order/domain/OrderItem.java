package com.realtimeanalytics.order.domain;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderItem(
    UUID id,
    UUID orderId,
    UUID productId,
    String productSku,
    String productTitle,
    BigDecimal unitPrice,
    int quantity,
    BigDecimal lineTotal
) {
    public static OrderItem create(UUID orderId, UUID productId, String productSku, String productTitle, BigDecimal unitPrice, int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }
        return new OrderItem(
            UUID.randomUUID(),
            orderId,
            productId,
            productSku,
            productTitle,
            unitPrice,
            quantity,
            unitPrice.multiply(BigDecimal.valueOf(quantity))
        );
    }
}
