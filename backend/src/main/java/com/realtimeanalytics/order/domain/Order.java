package com.realtimeanalytics.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Order(
    UUID id,
    UUID customerId,
    String status,
    BigDecimal subtotal,
    BigDecimal taxAmount,
    BigDecimal shippingAmount,
    BigDecimal totalAmount,
    Instant createdAt,
    Instant updatedAt
) {
    public static Order create(UUID customerId, BigDecimal subtotal, BigDecimal taxAmount, BigDecimal shippingAmount) {
        return new Order(
            UUID.randomUUID(),
            customerId,
            "PENDING",
            subtotal,
            taxAmount,
            shippingAmount,
            subtotal.add(taxAmount).add(shippingAmount),
            Instant.now(),
            Instant.now()
        );
    }
}
