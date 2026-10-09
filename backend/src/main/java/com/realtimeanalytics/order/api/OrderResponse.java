package com.realtimeanalytics.order.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(
    UUID id,
    UUID customerId,
    String status,
    BigDecimal subtotal,
    BigDecimal taxAmount,
    BigDecimal shippingAmount,
    BigDecimal totalAmount,
    Instant createdAt,
    List<OrderItemResponse> items
) {}
