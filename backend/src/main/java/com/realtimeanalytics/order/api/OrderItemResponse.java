package com.realtimeanalytics.order.api;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderItemResponse(
    UUID id,
    UUID productId,
    String productSku,
    String productTitle,
    BigDecimal unitPrice,
    int quantity,
    BigDecimal lineTotal
) {}
