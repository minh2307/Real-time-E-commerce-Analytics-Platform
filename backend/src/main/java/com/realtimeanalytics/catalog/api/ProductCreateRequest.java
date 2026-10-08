package com.realtimeanalytics.catalog.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.UUID;

public record ProductCreateRequest(
    @NotNull UUID categoryId,
    @NotBlank String sku,
    @NotBlank String title,
    String description,
    @NotNull @DecimalMin("0.0") BigDecimal price
) {}
