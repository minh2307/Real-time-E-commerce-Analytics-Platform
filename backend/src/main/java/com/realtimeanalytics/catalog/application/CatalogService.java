package com.realtimeanalytics.catalog.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    private final ObjectMapper objectMapper;

    public CatalogService(CategoryRepository categoryRepository, ProductRepository productRepository, OutboxService outboxService, ObjectMapper objectMapper) {
        this.categoryRepository = categoryRepository;
        this.productRepository = productRepository;
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
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
        
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("categoryId", category.id().toString());
        payload.put("name", category.name());
        payload.put("slug", category.slug());
        if (category.parentId() != null) {
            payload.put("parentCategoryId", category.parentId().toString());
        }
        payload.put("description", category.description());
        outboxService.saveEvent("Category", category.id(), "CATEGORY_CREATED", payload.toString());
        
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
        
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("productId", product.id().toString());
        payload.put("sku", product.sku());
        payload.put("title", product.title());
        payload.put("description", product.description());
        payload.put("categoryId", product.categoryId().toString());
        payload.put("currentPrice", product.price().doubleValue());
        payload.put("isActive", true);
        outboxService.saveEvent("Product", product.id(), "PRODUCT_CREATED", payload.toString());
        
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
