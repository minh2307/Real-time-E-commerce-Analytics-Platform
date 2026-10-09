package com.realtimeanalytics.order.api;

import com.realtimeanalytics.order.application.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    private String getUserEmail(Authentication authentication) {
        return authentication.getName();
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
            Authentication authentication,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody OrderCreateRequest request) {
        try {
            String email = getUserEmail(authentication);
            OrderResponse response = orderService.createOrder(email, idempotencyKey, request);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(
            Authentication authentication,
            @PathVariable UUID orderId) {
        try {
            String email = getUserEmail(authentication);
            OrderResponse response = orderService.getOrder(email, orderId);
            return ResponseEntity.ok(response);
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping
    public ResponseEntity<PaginatedOrderResponse> getOrderHistory(
            Authentication authentication,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        String email = getUserEmail(authentication);
        PaginatedOrderResponse response = orderService.getOrderHistory(email, page, size);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{orderId}/cancel")
    public ResponseEntity<OrderResponse> cancelOrder(
            Authentication authentication,
            @PathVariable UUID orderId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        try {
            String email = getUserEmail(authentication);
            OrderResponse response = orderService.cancelOrder(email, idempotencyKey, orderId);
            return ResponseEntity.ok(response);
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }
}
