package com.realtimeanalytics.inventory.api;

import com.realtimeanalytics.inventory.application.InventoryService;
import com.realtimeanalytics.inventory.application.OptimisticLockingFailureException;
import com.realtimeanalytics.inventory.domain.Inventory;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/admin/inventory")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @PutMapping("/{productId}")
    public ResponseEntity<?> restockInventory(
            @PathVariable UUID productId,
            @Valid @RequestBody InventoryRestockRequest request) {
        try {
            Inventory updated = inventoryService.restockInventory(productId, request.additionalQuantity());
            return ResponseEntity.ok(InventoryResponse.fromDomain(updated));
        } catch (OptimisticLockingFailureException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
        }
    }
}
