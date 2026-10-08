package com.realtimeanalytics.catalog.api;

import com.realtimeanalytics.catalog.application.CatalogService;
import com.realtimeanalytics.catalog.domain.Category;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class CategoryController {

    private final CatalogService catalogService;

    public CategoryController(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GetMapping("/categories")
    public PaginatedResponse<CategoryResponse> listCategories(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        List<Category> categories = catalogService.listCategories(page, size);
        List<CategoryResponse> responses = categories.stream().map(CategoryResponse::fromDomain).toList();
        return new PaginatedResponse<>(responses, page, size);
    }

    @GetMapping("/categories/{categoryId}")
    public ResponseEntity<CategoryResponse> getCategory(@PathVariable UUID categoryId) {
        return catalogService.getCategory(categoryId)
                .map(CategoryResponse::fromDomain)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/admin/categories")
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryResponse createCategory(@Valid @RequestBody CategoryCreateRequest request) {
        Category category = catalogService.createCategory(
                request.parentId(),
                request.slug(),
                request.name(),
                request.description()
        );
        return CategoryResponse.fromDomain(category);
    }
}
