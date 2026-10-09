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
import com.realtimeanalytics.order.api.OrderResponse;
import com.realtimeanalytics.order.domain.Order;
import com.realtimeanalytics.order.domain.OrderRepository;
import com.realtimeanalytics.outbox.application.OutboxService;
import com.realtimeanalytics.user.domain.User;
import com.realtimeanalytics.user.domain.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderServiceTest {

    private OrderRepository orderRepository;
    private CartRepository cartRepository;
    private CartItemRepository cartItemRepository;
    private CartService cartService;
    private CatalogService catalogService;
    private InventoryService inventoryService;
    private OutboxService outboxService;
    private UserRepository userRepository;
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderRepository = mock(OrderRepository.class);
        cartRepository = mock(CartRepository.class);
        cartItemRepository = mock(CartItemRepository.class);
        cartService = mock(CartService.class);
        catalogService = mock(CatalogService.class);
        inventoryService = mock(InventoryService.class);
        outboxService = mock(OutboxService.class);
        userRepository = mock(UserRepository.class);
        
        orderService = new OrderService(orderRepository, cartRepository, cartItemRepository,
                cartService, catalogService, inventoryService, outboxService, userRepository);
    }

    private User mockUser(String email) {
        User user = new User(UUID.randomUUID(), email, "hash", "F", "L", "USER", OffsetDateTime.now(), OffsetDateTime.now());
        when(userRepository.findByEmail(email)).thenReturn(Optional.of(user));
        return user;
    }

    @Test
    void createOrder_Success() {
        String email = "test@test.com";
        User user = mockUser(email);
        
        UUID productId = UUID.randomUUID();
        Cart cart = new Cart(UUID.randomUUID(), user.id(), Instant.now(), Instant.now());
        CartItem cartItem = new CartItem(UUID.randomUUID(), cart.id(), productId, 2, Instant.now());
        
        Product product = new Product(productId, UUID.randomUUID(), "SKU", "Title", "Desc", BigDecimal.TEN, Instant.now(), Instant.now());
        
        when(cartRepository.findByUserId(user.id())).thenReturn(Optional.of(cart));
        when(cartItemRepository.findByCartId(cart.id())).thenReturn(Collections.singletonList(cartItem));
        when(catalogService.getProduct(productId)).thenReturn(Optional.of(product));
        
        OrderCreateRequest request = new OrderCreateRequest(null, "Shipping", "Billing");
        
        OrderResponse response = orderService.createOrder(email, "idemp-key", request);
        
        assertNotNull(response);
        assertEquals(user.id(), response.customerId());
        assertEquals(1, response.items().size());
        assertEquals(0, BigDecimal.valueOf(20).compareTo(response.totalAmount()));
        
        verify(inventoryService).decrementInventory(productId, 2);
        verify(orderRepository).save(any(), any());
        verify(cartService).clearCart(email);
        verify(outboxService).saveEvent(eq("Order"), any(), eq("ORDER_CREATED"), anyString());
    }

    @Test
    void createOrder_InsufficientInventory_ThrowsException() {
        String email = "test@test.com";
        User user = mockUser(email);
        
        UUID productId = UUID.randomUUID();
        Cart cart = new Cart(UUID.randomUUID(), user.id(), Instant.now(), Instant.now());
        CartItem cartItem = new CartItem(UUID.randomUUID(), cart.id(), productId, 2, Instant.now());
        
        Product product = new Product(productId, UUID.randomUUID(), "SKU", "Title", "Desc", BigDecimal.TEN, Instant.now(), Instant.now());
        
        when(cartRepository.findByUserId(user.id())).thenReturn(Optional.of(cart));
        when(cartItemRepository.findByCartId(cart.id())).thenReturn(Collections.singletonList(cartItem));
        when(catalogService.getProduct(productId)).thenReturn(Optional.of(product));
        
        doThrow(new IllegalStateException("Insufficient inventory")).when(inventoryService).decrementInventory(productId, 2);
        
        OrderCreateRequest request = new OrderCreateRequest(null, "Shipping", "Billing");
        
        assertThrows(IllegalStateException.class, () -> orderService.createOrder(email, "idemp-key", request));
        
        verify(orderRepository, never()).save(any(), any());
        verify(cartService, never()).clearCart(anyString());
    }

    @Test
    void cancelOrder_Success() {
        String email = "test@test.com";
        User user = mockUser(email);
        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId, user.id(), "PENDING", BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.TEN, Instant.now(), Instant.now());
        
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(orderRepository.findItemsByOrderId(orderId)).thenReturn(Collections.emptyList());
        
        OrderResponse response = orderService.cancelOrder(email, "idemp-key", orderId);
        
        assertNotNull(response);
        verify(orderRepository).updateStatus(orderId, "CANCELLED");
        verify(outboxService).saveEvent(eq("Order"), eq(orderId), eq("ORDER_CANCELLED"), anyString());
    }
}
