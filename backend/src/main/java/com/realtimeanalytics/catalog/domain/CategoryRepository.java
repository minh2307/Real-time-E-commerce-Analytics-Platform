package com.realtimeanalytics.catalog.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CategoryRepository {
    void save(Category category);
    Optional<Category> findById(UUID id);
    List<Category> findAll(int offset, int limit);
}
