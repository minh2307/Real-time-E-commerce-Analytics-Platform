package com.realtimeanalytics.cart.api;

import java.math.BigDecimal;
import java.util.UUID;

public record CartItemResponse(
    UUID productId,
    String sku,
    String title,
    BigDecimal price,
    int quantity,
    BigDecimal subtotal
) {}
