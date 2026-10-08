package com.realtimeanalytics.catalog.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ProductRepository {
    void save(Product product);
    Optional<Product> findById(UUID id);
    List<Product> findByCategoryId(UUID categoryId, int offset, int limit);
    List<Product> findAll(int offset, int limit);
}
