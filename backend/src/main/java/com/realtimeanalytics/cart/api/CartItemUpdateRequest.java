package com.realtimeanalytics.cart.api;

import jakarta.validation.constraints.Min;

public record CartItemUpdateRequest(
    @Min(1) int quantity
) {}
