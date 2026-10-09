package com.realtimeanalytics.payment.api;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentResponse(
        UUID paymentId,
        UUID orderId,
        String status,
        BigDecimal amount,
        String currency,
        String errorCode,
        OffsetDateTime createdAt
) {}
