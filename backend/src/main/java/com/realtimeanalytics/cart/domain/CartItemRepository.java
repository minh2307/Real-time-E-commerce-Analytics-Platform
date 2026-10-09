package com.realtimeanalytics.cart.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CartItemRepository {
    void save(CartItem item);
    void updateQuantity(UUID id, int quantity);
    void delete(UUID id);
    void deleteByCartId(UUID cartId);
    Optional<CartItem> findByCartIdAndProductId(UUID cartId, UUID productId);
    List<CartItem> findByCartId(UUID cartId);
}
