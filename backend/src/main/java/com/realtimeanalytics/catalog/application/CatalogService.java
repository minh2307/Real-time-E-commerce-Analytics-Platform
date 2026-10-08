package com.realtimeanalytics.catalog.application;

import com.realtimeanalytics.catalog.domain.Category;
import com.realtimeanalytics.catalog.domain.CategoryRepository;
import com.realtimeanalytics.catalog.domain.Product;
import com.realtimeanalytics.catalog.domain.ProductRepository;
import com.realtimeanalytics.outbox.application.OutboxService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class CatalogService {

    private final CategoryRepository categoryRepository;
    private final ProductRepository productRepository;
    private final OutboxService outboxService;

    public CatalogService(CategoryRepository categoryRepository, ProductRepository productRepository, OutboxService outboxService) {
        this.categoryRepository = categoryRepository;
        this.productRepository = productRepository;
        this.outboxService = outboxService;
    }

    @Transactional(readOnly = true)
    public List<Category> listCategories(int page, int size) {
        int offset = page * size;
        return categoryRepository.findAll(offset, size);
    }

    @Transactional(readOnly = true)
    public Optional<Category> getCategory(UUID id) {
        return categoryRepository.findById(id);
    }

    @Transactional
    public Category createCategory(UUID parentId, String slug, String name, String description) {
        Category category = Category.create(parentId, slug, name, description);
        categoryRepository.save(category);
        return category;
    }

    @Transactional(readOnly = true)
    public List<Product> listProducts(UUID categoryId, int page, int size) {
        int offset = page * size;
        if (categoryId != null) {
            return productRepository.findByCategoryId(categoryId, offset, size);
        } else {
            return productRepository.findAll(offset, size);
        }
    }

    @Transactional(readOnly = true)
    public Optional<Product> getProduct(UUID id) {
        return productRepository.findById(id);
    }

    @Transactional
    public Product createProduct(UUID categoryId, String sku, String title, String description, BigDecimal price) {
        Product product = Product.create(categoryId, sku, title, description, price);
        productRepository.save(product);
        return product;
    }

    @Transactional
    public void recordProductView(UUID productId) {
        Optional<Product> product = productRepository.findById(productId);
        if (product.isEmpty()) {
            throw new IllegalArgumentException("Product not found");
        }
        
        String payload = String.format("{\"productId\":\"%s\"}", productId);
        outboxService.saveEvent("Product", productId, "PRODUCT_VIEWED", payload);
    }
}
