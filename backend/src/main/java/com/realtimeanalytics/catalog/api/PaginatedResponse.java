package com.realtimeanalytics.catalog.api;

import java.util.List;

public record PaginatedResponse<T>(
    List<T> data,
    int page,
    int size
) {}
