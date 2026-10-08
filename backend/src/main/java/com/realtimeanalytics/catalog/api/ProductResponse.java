package com.realtimeanalytics.catalog.api;

import com.realtimeanalytics.catalog.domain.Product;
import java.math.BigDecimal;
import java.util.UUID;

public record ProductResponse(
    UUID id,
    UUID categoryId,
    String sku,
    String title,
    String description,
    BigDecimal price
) {
    public static ProductResponse fromDomain(Product product) {
        return new ProductResponse(
            product.id(),
            product.categoryId(),
            product.sku(),
            product.title(),
            product.description(),
            product.price()
        );
    }
}
