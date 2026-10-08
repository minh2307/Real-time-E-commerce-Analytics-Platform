package com.realtimeanalytics.inventory.api;

import com.realtimeanalytics.inventory.domain.Inventory;
import java.util.UUID;

public record InventoryResponse(
    UUID productId,
    int availableQuantity,
    int reservedQuantity,
    long version
) {
    public static InventoryResponse fromDomain(Inventory inventory) {
        return new InventoryResponse(
            inventory.productId(),
            inventory.availableQuantity(),
            inventory.reservedQuantity(),
            inventory.version()
        );
    }
}
