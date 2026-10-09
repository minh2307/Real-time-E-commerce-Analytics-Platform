package com.realtimeanalytics.cart.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record CartItemAddRequest(
    @NotNull UUID productId,
    @Min(1) int quantity
) {}
