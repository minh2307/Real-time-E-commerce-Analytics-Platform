package com.realtimeanalytics.inventory.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record InventoryRestockRequest(
    @NotNull @Min(1) Integer additionalQuantity
) {}
