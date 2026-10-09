package com.realtimeanalytics.order.api;

import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

public record OrderCreateRequest(
    List<UUID> productIds,
    @NotNull String shippingAddress,
    @NotNull String billingAddress
) {}
