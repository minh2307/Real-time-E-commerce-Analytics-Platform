package com.realtimeanalytics.order.api;

import java.util.List;

public record PaginatedOrderResponse(
    List<OrderResponse> content,
    int page,
    int size
) {}
