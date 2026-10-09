package com.realtimeanalytics.payment.domain;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Payment(
        UUID id,
        UUID orderId,
        int attemptNumber,
        String status,
        BigDecimal amount,
        String currency,
        String paymentMethod,
        String errorCode,
        String idempotencyKey,
        OffsetDateTime createdAt
) {
    public static Payment create(UUID orderId, int attemptNumber, BigDecimal amount, String currency, String paymentMethod, String idempotencyKey) {
        return new Payment(
                null,
                orderId,
                attemptNumber,
                "PENDING",
                amount,
                currency,
                paymentMethod,
                null,
                idempotencyKey,
                null
        );
    }
}
