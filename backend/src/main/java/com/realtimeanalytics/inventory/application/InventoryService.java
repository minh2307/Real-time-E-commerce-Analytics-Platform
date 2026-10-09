package com.realtimeanalytics.inventory.application;

import com.realtimeanalytics.inventory.domain.Inventory;
import com.realtimeanalytics.inventory.domain.InventoryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    public InventoryService(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
    }

    @Transactional
    public Inventory restockInventory(UUID productId, int additionalQuantity) {
        if (additionalQuantity <= 0) {
            throw new IllegalArgumentException("Quantity to restock must be positive");
        }

        Optional<Inventory> opt = inventoryRepository.findByProductId(productId);
        if (opt.isPresent()) {
            Inventory current = opt.get();
            Inventory updated = new Inventory(
                    current.productId(),
                    current.availableQuantity() + additionalQuantity,
                    current.reservedQuantity(),
                    current.version(),
                    current.updatedAt()
            );
            boolean success = inventoryRepository.update(updated);
            if (!success) {
                throw new OptimisticLockingFailureException("Inventory was updated concurrently. Please retry.");
            }
            return inventoryRepository.findByProductId(productId).orElseThrow();
        } else {
            Inventory newInventory = Inventory.create(productId, additionalQuantity);
            inventoryRepository.save(newInventory);
            return newInventory;
        }
    }

    @Transactional
    public void decrementInventory(UUID productId, int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be positive");
        }
        
        int retries = 3;
        while (retries > 0) {
            Inventory current = inventoryRepository.findByProductId(productId)
                    .orElseThrow(() -> new IllegalArgumentException("Inventory not found for product"));
            
            if (current.availableQuantity() < quantity) {
                throw new IllegalStateException("Insufficient inventory");
            }
            
            Inventory updated = new Inventory(
                    current.productId(),
                    current.availableQuantity() - quantity,
                    current.reservedQuantity(),
                    current.version(),
                    current.updatedAt()
            );
            
            if (inventoryRepository.update(updated)) {
                return;
            }
            retries--;
        }
        throw new OptimisticLockingFailureException("Failed to update inventory due to concurrent modifications");
    }

    @Transactional
    public void restoreInventory(UUID productId, int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be positive");
        }

        int retries = 3;
        while (retries > 0) {
            Inventory current = inventoryRepository.findByProductId(productId)
                    .orElseThrow(() -> new IllegalArgumentException("Inventory not found for product"));
            
            Inventory updated = new Inventory(
                    current.productId(),
                    current.availableQuantity() + quantity,
                    current.reservedQuantity(),
                    current.version(),
                    current.updatedAt()
            );
            
            if (inventoryRepository.update(updated)) {
                return;
            }
            retries--;
        }
        throw new OptimisticLockingFailureException("Failed to restore inventory due to concurrent modifications");
    }
}
