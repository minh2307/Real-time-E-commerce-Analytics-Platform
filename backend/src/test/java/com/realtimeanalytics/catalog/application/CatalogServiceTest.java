package com.realtimeanalytics.catalog.application;

import com.realtimeanalytics.catalog.domain.Category;
import com.realtimeanalytics.catalog.domain.CategoryRepository;
import com.realtimeanalytics.catalog.domain.Product;
import com.realtimeanalytics.catalog.domain.ProductRepository;
import com.realtimeanalytics.outbox.application.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CatalogServiceTest {

    private CategoryRepository categoryRepository;
    private ProductRepository productRepository;
    private OutboxService outboxService;
    private CatalogService catalogService;

    @BeforeEach
    void setUp() {
        categoryRepository = mock(CategoryRepository.class);
        productRepository = mock(ProductRepository.class);
        outboxService = mock(OutboxService.class);
        catalogService = new CatalogService(categoryRepository, productRepository, outboxService);
    }

    @Test
    void createProduct_Success() {
        UUID catId = UUID.randomUUID();
        Product product = catalogService.createProduct(catId, "SKU1", "T1", "D1", new BigDecimal("10.0"));
        
        assertNotNull(product);
        assertEquals("SKU1", product.sku());
        verify(productRepository).save(any(Product.class));
    }

    @Test
    void createProduct_NegativePrice_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> 
            catalogService.createProduct(UUID.randomUUID(), "SKU", "Title", "Desc", new BigDecimal("-1.0"))
        );
    }

    @Test
    void recordProductView_Success() {
        UUID productId = UUID.randomUUID();
        Product product = Product.create(UUID.randomUUID(), "SKU", "Title", "Desc", BigDecimal.TEN);
        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        
        catalogService.recordProductView(productId);
        
        verify(outboxService).saveEvent(eq("Product"), eq(productId), eq("PRODUCT_VIEWED"), anyString());
    }

    @Test
    void recordProductView_NotFound() {
        UUID productId = UUID.randomUUID();
        when(productRepository.findById(productId)).thenReturn(Optional.empty());
        
        assertThrows(IllegalArgumentException.class, () -> catalogService.recordProductView(productId));
    }
}
