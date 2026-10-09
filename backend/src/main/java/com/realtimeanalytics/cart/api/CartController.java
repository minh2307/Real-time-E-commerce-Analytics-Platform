package com.realtimeanalytics.cart.api;

import com.realtimeanalytics.cart.application.CartService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/cart")
public class CartController {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    private String getUserEmail(Authentication authentication) {
        return authentication.getName();
    }

    @GetMapping
    public ResponseEntity<CartResponse> getCart(Authentication authentication) {
        String email = getUserEmail(authentication);
        return ResponseEntity.ok(cartService.getCart(email));
    }

    @PostMapping("/items")
    public ResponseEntity<CartResponse> addItem(
            Authentication authentication,
            @Valid @RequestBody CartItemAddRequest request) {
        try {
            String email = getUserEmail(authentication);
            CartResponse response = cartService.addItem(email, request.productId(), request.quantity());
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PutMapping("/items/{productId}")
    public ResponseEntity<CartResponse> updateItemQuantity(
            Authentication authentication,
            @PathVariable UUID productId,
            @Valid @RequestBody CartItemUpdateRequest request) {
        try {
            String email = getUserEmail(authentication);
            CartResponse response = cartService.updateItemQuantity(email, productId, request.quantity());
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @DeleteMapping("/items/{productId}")
    public ResponseEntity<CartResponse> removeItem(
            Authentication authentication,
            @PathVariable UUID productId) {
        String email = getUserEmail(authentication);
        CartResponse response = cartService.removeItem(email, productId);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping
    public ResponseEntity<Void> clearCart(Authentication authentication) {
        String email = getUserEmail(authentication);
        cartService.clearCart(email);
        return ResponseEntity.noContent().build();
    }
}
