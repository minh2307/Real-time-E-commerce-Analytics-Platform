package com.realtimeanalytics.order.application;

import com.realtimeanalytics.cart.application.CartService;
import com.realtimeanalytics.cart.domain.Cart;
import com.realtimeanalytics.cart.domain.CartItem;
import com.realtimeanalytics.cart.domain.CartItemRepository;
import com.realtimeanalytics.cart.domain.CartRepository;
import com.realtimeanalytics.catalog.application.CatalogService;
import com.realtimeanalytics.catalog.domain.Product;
import com.realtimeanalytics.inventory.application.InventoryService;
import com.realtimeanalytics.inventory.application.OptimisticLockingFailureException;
import com.realtimeanalytics.order.api.OrderCreateRequest;
import com.realtimeanalytics.order.api.OrderItemResponse;
import com.realtimeanalytics.order.api.OrderResponse;
import com.realtimeanalytics.order.api.PaginatedOrderResponse;
import com.realtimeanalytics.order.domain.Order;
import com.realtimeanalytics.order.domain.OrderItem;
import com.realtimeanalytics.order.domain.OrderRepository;
import com.realtimeanalytics.outbox.application.OutboxService;
import com.realtimeanalytics.user.domain.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final CartService cartService;
    private final CatalogService catalogService;
    private final InventoryService inventoryService;
    private final OutboxService outboxService;
    private final UserRepository userRepository;

    public OrderService(OrderRepository orderRepository,
                        CartRepository cartRepository,
                        CartItemRepository cartItemRepository,
                        CartService cartService,
                        CatalogService catalogService,
                        InventoryService inventoryService,
                        OutboxService outboxService,
                        UserRepository userRepository) {
        this.orderRepository = orderRepository;
        this.cartRepository = cartRepository;
        this.cartItemRepository = cartItemRepository;
        this.cartService = cartService;
        this.catalogService = catalogService;
        this.inventoryService = inventoryService;
        this.outboxService = outboxService;
        this.userRepository = userRepository;
    }

    private UUID resolveUserId(String userEmail) {
        return userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new IllegalStateException("User not found"))
                .id();
    }

    @Transactional
    public OrderResponse createOrder(String userEmail, String idempotencyKey, OrderCreateRequest request) {
        UUID userId = resolveUserId(userEmail);
        
        Cart cart = cartRepository.findByUserId(userId)
                .orElseThrow(() -> new IllegalStateException("Cart is empty"));
                
        List<CartItem> cartItems = cartItemRepository.findByCartId(cart.id());
        
        if (cartItems.isEmpty()) {
            throw new IllegalStateException("Cart is empty");
        }
        
        BigDecimal subtotal = BigDecimal.ZERO;
        List<OrderItem> orderItems = new ArrayList<>();
        Order tempOrder = Order.create(userId, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
        
        for (CartItem cartItem : cartItems) {
            Product product = catalogService.getProduct(cartItem.productId())
                    .orElseThrow(() -> new IllegalStateException("Product not found: " + cartItem.productId()));
                    
            try {
                inventoryService.decrementInventory(product.id(), cartItem.quantity());
            } catch (OptimisticLockingFailureException | IllegalStateException e) {
                throw new IllegalStateException("Failed to reserve inventory for product: " + product.title(), e);
            }
            
            OrderItem orderItem = OrderItem.create(
                    tempOrder.id(),
                    product.id(),
                    product.sku(),
                    product.title(),
                    product.price(),
                    cartItem.quantity()
            );
            orderItems.add(orderItem);
            subtotal = subtotal.add(orderItem.lineTotal());
        }
        
        BigDecimal taxAmount = BigDecimal.ZERO;
        BigDecimal shippingAmount = BigDecimal.ZERO;
        
        Order finalOrder = new Order(
                tempOrder.id(),
                tempOrder.customerId(),
                tempOrder.status(),
                subtotal,
                taxAmount,
                shippingAmount,
                subtotal.add(taxAmount).add(shippingAmount),
                tempOrder.createdAt(),
                tempOrder.updatedAt()
        );
        
        orderRepository.save(finalOrder, orderItems);
        
        cartService.clearCart(userEmail);
        
        String payload = String.format("{\"orderId\":\"%s\",\"customerId\":\"%s\",\"totalAmount\":%s}", 
                finalOrder.id(), userId, finalOrder.totalAmount());
        outboxService.saveEvent("Order", finalOrder.id(), "ORDER_CREATED", payload);
        
        return buildOrderResponse(finalOrder, orderItems);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(String userEmail, UUID orderId) {
        UUID userId = resolveUserId(userEmail);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found"));
                
        if (!order.customerId().equals(userId)) {
            throw new SecurityException("Forbidden");
        }
        
        List<OrderItem> items = orderRepository.findItemsByOrderId(orderId);
        return buildOrderResponse(order, items);
    }

    @Transactional(readOnly = true)
    public PaginatedOrderResponse getOrderHistory(String userEmail, int page, int size) {
        UUID userId = resolveUserId(userEmail);
        int offset = page * size;
        List<Order> orders = orderRepository.findByCustomerId(userId, size, offset);
        
        List<OrderResponse> content = orders.stream().map(order -> {
            List<OrderItem> items = orderRepository.findItemsByOrderId(order.id());
            return buildOrderResponse(order, items);
        }).collect(Collectors.toList());
        
        return new PaginatedOrderResponse(content, page, size);
    }

    @Transactional
    public OrderResponse cancelOrder(String userEmail, String idempotencyKey, UUID orderId) {
        UUID userId = resolveUserId(userEmail);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found"));
                
        if (!order.customerId().equals(userId)) {
            throw new SecurityException("Forbidden");
        }
        
        if (!order.status().equals("PENDING") && !order.status().equals("PAYMENT_FAILED")) {
            throw new IllegalStateException("Order cannot be cancelled from status: " + order.status());
        }
        
        orderRepository.updateStatus(orderId, "CANCELLED");
        
        List<OrderItem> items = orderRepository.findItemsByOrderId(orderId);
        for (OrderItem item : items) {
            inventoryService.restoreInventory(item.productId(), item.quantity());
        }
        
        String payload = String.format("{\"orderId\":\"%s\"}", orderId);
        outboxService.saveEvent("Order", orderId, "ORDER_CANCELLED", payload);
        
        return getOrder(userEmail, orderId);
    }

    private OrderResponse buildOrderResponse(Order order, List<OrderItem> items) {
        List<OrderItemResponse> itemResponses = items.stream()
                .map(item -> new OrderItemResponse(
                        item.id(),
                        item.productId(),
                        item.productSku(),
                        item.productTitle(),
                        item.unitPrice(),
                        item.quantity(),
                        item.lineTotal()
                )).collect(Collectors.toList());
                
        return new OrderResponse(
                order.id(),
                order.customerId(),
                order.status(),
                order.subtotal(),
                order.taxAmount(),
                order.shippingAmount(),
                order.totalAmount(),
                order.createdAt(),
                itemResponses
        );
    }
}
