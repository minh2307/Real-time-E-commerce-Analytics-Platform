package com.realtimeanalytics.cart.application;

import com.realtimeanalytics.cart.api.CartItemResponse;
import com.realtimeanalytics.cart.api.CartResponse;
import com.realtimeanalytics.cart.domain.Cart;
import com.realtimeanalytics.cart.domain.CartItem;
import com.realtimeanalytics.cart.domain.CartItemRepository;
import com.realtimeanalytics.cart.domain.CartRepository;
import com.realtimeanalytics.catalog.application.CatalogService;
import com.realtimeanalytics.catalog.domain.Product;
import com.realtimeanalytics.outbox.application.OutboxService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class CartService {

    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final CatalogService catalogService;
    private final OutboxService outboxService;
    private final com.realtimeanalytics.user.domain.UserRepository userRepository;

    public CartService(CartRepository cartRepository, CartItemRepository cartItemRepository, 
                       CatalogService catalogService, OutboxService outboxService,
                       com.realtimeanalytics.user.domain.UserRepository userRepository) {
        this.cartRepository = cartRepository;
        this.cartItemRepository = cartItemRepository;
        this.catalogService = catalogService;
        this.outboxService = outboxService;
        this.userRepository = userRepository;
    }

    private UUID resolveUserId(String userEmail) {
        return userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new IllegalStateException("User not found"))
                .id();
    }

    @Transactional
    public CartResponse getCart(String userEmail) {
        UUID userId = resolveUserId(userEmail);
        Cart cart = getOrCreateCart(userId);
        return buildCartResponse(cart);
    }

    @Transactional
    public CartResponse addItem(String userEmail, UUID productId, int quantity) {
        UUID userId = resolveUserId(userEmail);
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }

        Product product = catalogService.getProduct(productId)
                .orElseThrow(() -> new IllegalArgumentException("Product not found"));

        Cart cart = getOrCreateCart(userId);

        Optional<CartItem> existingItem = cartItemRepository.findByCartIdAndProductId(cart.id(), productId);
        if (existingItem.isPresent()) {
            cartItemRepository.updateQuantity(existingItem.get().id(), existingItem.get().quantity() + quantity);
        } else {
            CartItem newItem = CartItem.create(cart.id(), productId, quantity);
            cartItemRepository.save(newItem);
        }

        cartRepository.updateTimestamp(cart.id());
        
        String payload = String.format("{\"userId\":\"%s\",\"productId\":\"%s\",\"quantity\":%d}", userId, productId, quantity);
        outboxService.saveEvent("Cart", cart.id(), "ITEM_ADDED_TO_CART", payload);

        return buildCartResponse(cart);
    }

    @Transactional
    public CartResponse updateItemQuantity(String userEmail, UUID productId, int quantity) {
        UUID userId = resolveUserId(userEmail);
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }

        Cart cart = getOrCreateCart(userId);
        CartItem existingItem = cartItemRepository.findByCartIdAndProductId(cart.id(), productId)
                .orElseThrow(() -> new IllegalArgumentException("Item not found in cart"));

        cartItemRepository.updateQuantity(existingItem.id(), quantity);
        cartRepository.updateTimestamp(cart.id());

        return buildCartResponse(cart);
    }

    @Transactional
    public CartResponse removeItem(String userEmail, UUID productId) {
        UUID userId = resolveUserId(userEmail);
        Cart cart = getOrCreateCart(userId);
        Optional<CartItem> existingItem = cartItemRepository.findByCartIdAndProductId(cart.id(), productId);
        
        if (existingItem.isPresent()) {
            cartItemRepository.delete(existingItem.get().id());
            cartRepository.updateTimestamp(cart.id());
            
            String payload = String.format("{\"userId\":\"%s\",\"productId\":\"%s\"}", userId, productId);
            outboxService.saveEvent("Cart", cart.id(), "ITEM_REMOVED_FROM_CART", payload);
        }

        return buildCartResponse(cart);
    }

    @Transactional
    public void clearCart(String userEmail) {
        UUID userId = resolveUserId(userEmail);
        Optional<Cart> cart = cartRepository.findByUserId(userId);
        if (cart.isPresent()) {
            cartItemRepository.deleteByCartId(cart.get().id());
            cartRepository.updateTimestamp(cart.get().id());
        }
    }

    private Cart getOrCreateCart(UUID userId) {
        return cartRepository.findByUserId(userId).orElseGet(() -> {
            Cart newCart = Cart.create(userId);
            cartRepository.save(newCart);
            return newCart;
        });
    }

    private CartResponse buildCartResponse(Cart cart) {
        List<CartItem> items = cartItemRepository.findByCartId(cart.id());
        
        BigDecimal totalPrice = BigDecimal.ZERO;
        List<CartItemResponse> itemResponses = new java.util.ArrayList<>();
        
        for (CartItem item : items) {
            Product product = catalogService.getProduct(item.productId()).orElse(null);
            if (product != null) {
                BigDecimal subtotal = product.price().multiply(BigDecimal.valueOf(item.quantity()));
                totalPrice = totalPrice.add(subtotal);
                
                itemResponses.add(new CartItemResponse(
                        product.id(),
                        product.sku(),
                        product.title(),
                        product.price(),
                        item.quantity(),
                        subtotal
                ));
            }
        }
        
        return new CartResponse(itemResponses, totalPrice);
    }
}
