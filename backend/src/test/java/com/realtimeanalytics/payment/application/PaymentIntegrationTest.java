package com.realtimeanalytics.payment.application;

import com.realtimeanalytics.order.domain.Order;
import com.realtimeanalytics.order.domain.OrderRepository;
import com.realtimeanalytics.outbox.domain.OutboxEvent;
import com.realtimeanalytics.outbox.domain.OutboxEventRepository;
import com.realtimeanalytics.payment.api.PaymentRequest;
import com.realtimeanalytics.payment.api.PaymentResponse;
import com.realtimeanalytics.payment.domain.Payment;
import com.realtimeanalytics.payment.domain.PaymentRepository;
import com.realtimeanalytics.user.domain.User;
import com.realtimeanalytics.user.domain.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class PaymentIntegrationTest {

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private UserRepository userRepository;

    private User testUser;
    private Order testOrder;

    @BeforeEach
    void setUp() {
        testUser = User.create(UUID.randomUUID().toString() + "@example.com", "password", "Test", "User");
        userRepository.save(testUser);
        testOrder = Order.create(testUser.id(), new BigDecimal("100.00"), BigDecimal.ZERO, BigDecimal.ZERO);
        orderRepository.save(testOrder, List.of());
    }

    @Test
    void testSuccessfulPayment_updatesOrderAndOutbox() {
        PaymentRequest request = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "CREDIT_CARD");
        String idempotencyKey = UUID.randomUUID().toString();

        PaymentResponse response = paymentService.processPayment(testUser.email(), idempotencyKey, request);

        assertEquals("SUCCESS", response.status());

        // Verify Order status
        Order updatedOrder = orderRepository.findById(testOrder.id()).orElseThrow();
        assertEquals("PAID", updatedOrder.status());

        // Verify Payment record
        Optional<Payment> payment = paymentRepository.findByIdempotencyKey(idempotencyKey);
        assertTrue(payment.isPresent());
        assertEquals("SUCCESS", payment.get().status());

        // Verify Outbox Event
        List<OutboxEvent> events = outboxEventRepository.findPendingEvents(100).stream()
                .filter(e -> e.getAggregateId().equals(testOrder.id()))
                .toList();
        assertFalse(events.isEmpty());
        assertTrue(events.stream().anyMatch(e -> e.getEventType().equals("ORDER_PAID")));
    }

    @Test
    void testFailedPayment_updatesOrderAndOutbox() {
        PaymentRequest request = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "FAIL");
        String idempotencyKey = UUID.randomUUID().toString();

        PaymentResponse response = paymentService.processPayment(testUser.email(), idempotencyKey, request);

        assertEquals("FAILED", response.status());

        // Verify Order status
        Order updatedOrder = orderRepository.findById(testOrder.id()).orElseThrow();
        assertEquals("PAYMENT_FAILED", updatedOrder.status());

        // Verify Outbox Event
        List<OutboxEvent> events = outboxEventRepository.findPendingEvents(100).stream()
                .filter(e -> e.getAggregateId().equals(testOrder.id()))
                .toList();
        assertFalse(events.isEmpty());
        assertTrue(events.stream().anyMatch(e -> e.getEventType().equals("ORDER_PAYMENT_FAILED")));
    }

    @Test
    void testIdempotency_sameKey_sameRequest() {
        PaymentRequest request = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "CREDIT_CARD");
        String idempotencyKey = UUID.randomUUID().toString();

        PaymentResponse response1 = paymentService.processPayment(testUser.email(), idempotencyKey, request);
        PaymentResponse response2 = paymentService.processPayment(testUser.email(), idempotencyKey, request);

        assertEquals(response1.paymentId(), response2.paymentId());
        assertEquals("SUCCESS", response1.status());

        // Verify only 1 payment was created
        assertEquals(1, paymentRepository.countByOrderId(testOrder.id()));
    }

    @Test
    void testIdempotency_sameKey_differentRequest_throwsException() {
        PaymentRequest request1 = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "CREDIT_CARD");
        String idempotencyKey = UUID.randomUUID().toString();

        paymentService.processPayment(testUser.email(), idempotencyKey, request1);

        PaymentRequest request2 = new PaymentRequest(testOrder.id(), new BigDecimal("200.00"), "USD", "CREDIT_CARD");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            paymentService.processPayment(testUser.email(), idempotencyKey, request2);
        });
        assertTrue(ex.getMessage().contains("different request parameters"));
    }

    @Test
    void testPayment_sameOrder_differentIdempotencyKeys_throwsException() {
        PaymentRequest request = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "CREDIT_CARD");
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        
        // First payment succeeds
        PaymentResponse response1 = paymentService.processPayment(testUser.email(), key1, request);
        assertEquals("SUCCESS", response1.status());
        
        // Second payment with different key fails due to business rule
        assertThrows(IllegalStateException.class, () -> {
            paymentService.processPayment(testUser.email(), key2, request);
        });
        
        // Verify only ONE payment was created
        assertEquals(1, paymentRepository.countByOrderId(testOrder.id()));
    }

    @Test
    void testConcurrentPayments_sameIdempotencyKey() throws InterruptedException {
        PaymentRequest request = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "CREDIT_CARD");
        String idempotencyKey = UUID.randomUUID().toString();

        int threadCount = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    latch.await();
                    PaymentResponse response = paymentService.processPayment(testUser.email(), idempotencyKey, request);
                    if (response != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    exceptionRef.set(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        latch.countDown(); // start all threads at once
        doneLatch.await();

        if (exceptionRef.get() != null) {
            try {
                java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.FileWriter("concurrency-exception.txt"));
                exceptionRef.get().printStackTrace(pw);
                pw.close();
            } catch (Exception e) {}
        }
        assertNull(exceptionRef.get(), "Exception occurred during concurrent execution: " + (exceptionRef.get() != null ? exceptionRef.get().getMessage() : ""));
        assertEquals(threadCount, successCount.get(), "All threads should return a successful response due to idempotency");
        
        // Verify only ONE payment was actually created in DB
        assertEquals(1, paymentRepository.countByOrderId(testOrder.id()));
    }

    @org.springframework.boot.test.mock.mockito.SpyBean
    private com.realtimeanalytics.order.application.OrderService orderServiceSpy;

    @Test
    void testRollbackOnOrderUpdateFailure() {
        PaymentRequest request = new PaymentRequest(testOrder.id(), new BigDecimal("100.00"), "USD", "CREDIT_CARD");
        String idempotencyKey = UUID.randomUUID().toString();

        org.mockito.Mockito.doThrow(new RuntimeException("DB Connection Lost")).when(orderServiceSpy).markOrderAsPaid(testOrder.id());

        assertThrows(RuntimeException.class, () -> {
            paymentService.processPayment(testUser.email(), idempotencyKey, request);
        });

        // Verify rollback: Payment was NOT saved
        Optional<Payment> payment = paymentRepository.findByIdempotencyKey(idempotencyKey);
        assertTrue(payment.isEmpty());
        
        // Order status is still PENDING
        Order updatedOrder = orderRepository.findById(testOrder.id()).orElseThrow();
        assertEquals("PENDING", updatedOrder.status());
    }
}
