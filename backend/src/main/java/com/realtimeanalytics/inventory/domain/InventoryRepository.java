package com.realtimeanalytics.inventory.domain;

import java.util.Optional;
import java.util.UUID;

public interface InventoryRepository {
    void save(Inventory inventory);
    Optional<Inventory> findByProductId(UUID productId);
    boolean update(Inventory inventory); // returns false if optimistic locking fails
}
