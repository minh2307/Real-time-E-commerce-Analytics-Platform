package com.realtimeanalytics.inventory.application;

import com.realtimeanalytics.inventory.domain.Inventory;
import com.realtimeanalytics.inventory.domain.InventoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class InventoryServiceTest {

    private InventoryRepository inventoryRepository;
    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryRepository = mock(InventoryRepository.class);
        inventoryService = new InventoryService(inventoryRepository);
    }

    @Test
    void restockInventory_NewProduct() {
        UUID productId = UUID.randomUUID();
        when(inventoryRepository.findByProductId(productId)).thenReturn(Optional.empty());

        Inventory result = inventoryService.restockInventory(productId, 10);
        
        assertNotNull(result);
        assertEquals(10, result.availableQuantity());
        verify(inventoryRepository).save(any(Inventory.class));
    }

    @Test
    void restockInventory_ExistingProduct() {
        UUID productId = UUID.randomUUID();
        Inventory current = Inventory.create(productId, 5);
        when(inventoryRepository.findByProductId(productId)).thenReturn(Optional.of(current))
                                                          .thenReturn(Optional.of(new Inventory(productId, 15, 0, 1L, current.updatedAt())));
        when(inventoryRepository.update(any(Inventory.class))).thenReturn(true);

        Inventory result = inventoryService.restockInventory(productId, 10);
        
        assertEquals(15, result.availableQuantity());
        verify(inventoryRepository).update(any(Inventory.class));
    }

    @Test
    void restockInventory_ConcurrentUpdate_ThrowsException() {
        UUID productId = UUID.randomUUID();
        Inventory current = Inventory.create(productId, 5);
        when(inventoryRepository.findByProductId(productId)).thenReturn(Optional.of(current));
        when(inventoryRepository.update(any(Inventory.class))).thenReturn(false); // Simulate optimistic lock failure

        assertThrows(OptimisticLockingFailureException.class, () -> inventoryService.restockInventory(productId, 10));
    }

    @Test
    void restockInventory_NegativeQuantity_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> inventoryService.restockInventory(UUID.randomUUID(), -5));
    }
}
