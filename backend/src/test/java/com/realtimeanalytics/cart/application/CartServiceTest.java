package com.realtimeanalytics.cart.application;

import com.realtimeanalytics.cart.api.CartResponse;
import com.realtimeanalytics.cart.domain.Cart;
import com.realtimeanalytics.cart.domain.CartItem;
import com.realtimeanalytics.cart.domain.CartItemRepository;
import com.realtimeanalytics.cart.domain.CartRepository;
import com.realtimeanalytics.catalog.application.CatalogService;
import com.realtimeanalytics.catalog.domain.Product;
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

class CartServiceTest {

    private CartRepository cartRepository;
    private CartItemRepository cartItemRepository;
    private CatalogService catalogService;
    private OutboxService outboxService;
    private UserRepository userRepository;
    private CartService cartService;

    @BeforeEach
    void setUp() {
        cartRepository = mock(CartRepository.class);
        cartItemRepository = mock(CartItemRepository.class);
        catalogService = mock(CatalogService.class);
        outboxService = mock(OutboxService.class);
        userRepository = mock(UserRepository.class);
        cartService = new CartService(cartRepository, cartItemRepository, catalogService, outboxService, userRepository);
    }

    private User mockUser(String email) {
        User user = new User(UUID.randomUUID(), email, "hash", "F", "L", "USER", OffsetDateTime.now(), OffsetDateTime.now());
        when(userRepository.findByEmail(email)).thenReturn(Optional.of(user));
        return user;
    }

    @Test
    void getCart_EmptyCart() {
        String email = "test@test.com";
        User user = mockUser(email);
        
        when(cartRepository.findByUserId(user.id())).thenReturn(Optional.empty());
        
        CartResponse response = cartService.getCart(email);
        
        assertNotNull(response);
        assertTrue(response.items().isEmpty());
        assertEquals(BigDecimal.ZERO, response.totalPrice());
        verify(cartRepository).save(any(Cart.class));
    }

    @Test
    void addItem_NewItem_Success() {
        String email = "test@test.com";
        User user = mockUser(email);
        UUID productId = UUID.randomUUID();
        Cart cart = new Cart(UUID.randomUUID(), user.id(), Instant.now(), Instant.now());
        Product product = new Product(productId, UUID.randomUUID(), "SKU", "T", "D", BigDecimal.TEN, Instant.now(), Instant.now());
        
        when(cartRepository.findByUserId(user.id())).thenReturn(Optional.of(cart));
        when(catalogService.getProduct(productId)).thenReturn(Optional.of(product));
        when(cartItemRepository.findByCartIdAndProductId(cart.id(), productId)).thenReturn(Optional.empty());
        when(cartItemRepository.findByCartId(cart.id())).thenReturn(Collections.singletonList(
                new CartItem(UUID.randomUUID(), cart.id(), productId, 2, Instant.now())
        ));
        
        CartResponse response = cartService.addItem(email, productId, 2);
        
        assertNotNull(response);
        assertEquals(1, response.items().size());
        assertEquals(0, BigDecimal.valueOf(20).compareTo(response.totalPrice()));
        
        verify(cartItemRepository).save(any(CartItem.class));
        verify(outboxService).saveEvent(eq("Cart"), eq(cart.id()), eq("ITEM_ADDED_TO_CART"), anyString());
    }

    @Test
    void addItem_ProductNotFound_ThrowsException() {
        String email = "test@test.com";
        mockUser(email);
        UUID productId = UUID.randomUUID();
        when(catalogService.getProduct(productId)).thenReturn(Optional.empty());
        
        assertThrows(IllegalArgumentException.class, () -> cartService.addItem(email, productId, 1));
    }

    @Test
    void removeItem_Success() {
        String email = "test@test.com";
        User user = mockUser(email);
        UUID productId = UUID.randomUUID();
        Cart cart = new Cart(UUID.randomUUID(), user.id(), Instant.now(), Instant.now());
        CartItem item = new CartItem(UUID.randomUUID(), cart.id(), productId, 2, Instant.now());
        
        when(cartRepository.findByUserId(user.id())).thenReturn(Optional.of(cart));
        when(cartItemRepository.findByCartIdAndProductId(cart.id(), productId)).thenReturn(Optional.of(item));
        when(cartItemRepository.findByCartId(cart.id())).thenReturn(Collections.emptyList());
        
        CartResponse response = cartService.removeItem(email, productId);
        
        assertNotNull(response);
        assertTrue(response.items().isEmpty());
        verify(cartItemRepository).delete(item.id());
        verify(outboxService).saveEvent(eq("Cart"), eq(cart.id()), eq("ITEM_REMOVED_FROM_CART"), anyString());
    }
}
