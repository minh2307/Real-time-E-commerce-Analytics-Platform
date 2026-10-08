package com.realtimeanalytics.catalog.api;

import com.realtimeanalytics.catalog.domain.Category;
import java.util.UUID;

public record CategoryResponse(
    UUID id,
    UUID parentId,
    String slug,
    String name,
    String description
) {
    public static CategoryResponse fromDomain(Category category) {
        return new CategoryResponse(
            category.id(),
            category.parentId(),
            category.slug(),
            category.name(),
            category.description()
        );
    }
}
