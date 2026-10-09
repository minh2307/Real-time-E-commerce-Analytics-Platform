package com.realtimeanalytics.cart.domain;

import java.util.Optional;
import java.util.UUID;

public interface CartRepository {
    void save(Cart cart);
    Optional<Cart> findByUserId(UUID userId);
    void updateTimestamp(UUID cartId);
    void deleteByUserId(UUID userId);
}
