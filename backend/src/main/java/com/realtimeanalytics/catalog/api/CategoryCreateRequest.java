package com.realtimeanalytics.catalog.api;

import jakarta.validation.constraints.NotBlank;
import java.util.UUID;

public record CategoryCreateRequest(
    UUID parentId,
    @NotBlank String slug,
    @NotBlank String name,
    String description
) {}
