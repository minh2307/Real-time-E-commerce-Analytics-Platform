package com.realtimeanalytics.order.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository {
    void save(Order order, List<OrderItem> items);
    Optional<Order> findById(UUID id);
    List<OrderItem> findItemsByOrderId(UUID orderId);
    List<Order> findByCustomerId(UUID customerId, int limit, int offset);
    void updateStatus(UUID orderId, String status);
}
