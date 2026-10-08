package com.realtimeanalytics.catalog.api;

import com.realtimeanalytics.catalog.application.CatalogService;
import com.realtimeanalytics.catalog.domain.Product;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class ProductController {

    private final CatalogService catalogService;

    public ProductController(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GetMapping("/products")
    public PaginatedResponse<ProductResponse> listProducts(
            @RequestParam(required = false) UUID categoryId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        List<Product> products = catalogService.listProducts(categoryId, page, size);
        List<ProductResponse> responses = products.stream().map(ProductResponse::fromDomain).toList();
        return new PaginatedResponse<>(responses, page, size);
    }

    @GetMapping("/products/{productId}")
    public ResponseEntity<ProductResponse> getProduct(@PathVariable UUID productId) {
        return catalogService.getProduct(productId)
                .map(ProductResponse::fromDomain)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/products/{productId}/views")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void recordProductView(@PathVariable UUID productId) {
        try {
            catalogService.recordProductView(productId);
        } catch (IllegalArgumentException e) {
            // If product doesn't exist, we can ignore or return 404. Based on requirements, return 404.
            throw new ProductNotFoundException();
        }
    }

    @PostMapping("/admin/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductResponse createProduct(@Valid @RequestBody ProductCreateRequest request) {
        Product product = catalogService.createProduct(
                request.categoryId(),
                request.sku(),
                request.title(),
                request.description(),
                request.price()
        );
        return ProductResponse.fromDomain(product);
    }
}
