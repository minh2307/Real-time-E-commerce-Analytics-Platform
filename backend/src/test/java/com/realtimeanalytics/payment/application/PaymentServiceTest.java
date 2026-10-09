package com.realtimeanalytics.payment.application;

import com.realtimeanalytics.order.application.OrderService;
import com.realtimeanalytics.payment.api.PaymentRequest;
import com.realtimeanalytics.payment.api.PaymentResponse;
import com.realtimeanalytics.payment.domain.Payment;
import com.realtimeanalytics.payment.domain.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private OrderService orderService;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentService = new PaymentService(paymentRepository, orderService);
    }

    @Test
    void processPayment_success_marksOrderAsPaid() {
        UUID orderId = UUID.randomUUID();
        String userEmail = "test@example.com";
        BigDecimal amount = new BigDecimal("100.00");
        
        when(paymentRepository.findByIdempotencyKey("idempotency-key")).thenReturn(Optional.empty());
        when(paymentRepository.findSuccessfulPaymentByOrderId(orderId)).thenReturn(Optional.empty());
        doNothing().when(orderService).validateOrderForPayment(userEmail, orderId, amount);
        when(paymentRepository.countByOrderId(orderId)).thenReturn(0);
        
        Payment savedPayment = new Payment(UUID.randomUUID(), orderId, 1, "SUCCESS", amount, "USD", "CREDIT_CARD", null, null, OffsetDateTime.now());
        when(paymentRepository.saveIfAbsent(any(Payment.class))).thenReturn(Optional.of(savedPayment));
        
        PaymentRequest request = new PaymentRequest(orderId, amount, "USD", "CREDIT_CARD");
        PaymentResponse response = paymentService.processPayment(userEmail, "idempotency-key", request);
        
        assertEquals("SUCCESS", response.status());
        verify(orderService).markOrderAsPaid(orderId);
        verify(orderService, never()).markOrderAsPaymentFailed(orderId);
    }

    @Test
    void processPayment_failure_marksOrderAsFailed() {
        UUID orderId = UUID.randomUUID();
        String userEmail = "test@example.com";
        BigDecimal amount = new BigDecimal("100.00");
        
        when(paymentRepository.findByIdempotencyKey("idempotency-key")).thenReturn(Optional.empty());
        when(paymentRepository.findSuccessfulPaymentByOrderId(orderId)).thenReturn(Optional.empty());
        doNothing().when(orderService).validateOrderForPayment(userEmail, orderId, amount);
        when(paymentRepository.countByOrderId(orderId)).thenReturn(0);
        
        Payment savedPayment = new Payment(UUID.randomUUID(), orderId, 1, "FAILED", amount, "USD", "FAIL", "DECLINED", null, OffsetDateTime.now());
        when(paymentRepository.saveIfAbsent(any(Payment.class))).thenReturn(Optional.of(savedPayment));
        
        PaymentRequest request = new PaymentRequest(orderId, amount, "USD", "FAIL");
        PaymentResponse response = paymentService.processPayment(userEmail, "idempotency-key", request);
        
        assertEquals("FAILED", response.status());
        verify(orderService, never()).markOrderAsPaid(orderId);
        verify(orderService).markOrderAsPaymentFailed(orderId);
    }
    
    @Test
    void processPayment_amountMismatch_throwsException() {
        UUID orderId = UUID.randomUUID();
        String userEmail = "test@example.com";
        BigDecimal amount = new BigDecimal("100.00");
        
        when(paymentRepository.findByIdempotencyKey("idempotency-key")).thenReturn(Optional.empty());
        when(paymentRepository.findSuccessfulPaymentByOrderId(orderId)).thenReturn(Optional.empty());
        doThrow(new IllegalArgumentException("Payment amount does not match order total"))
            .when(orderService).validateOrderForPayment(userEmail, orderId, amount);
        
        PaymentRequest request = new PaymentRequest(orderId, amount, "USD", "CREDIT_CARD");
        
        assertThrows(IllegalArgumentException.class, () -> 
            paymentService.processPayment(userEmail, "idempotency-key", request)
        );
    }

    @Test
    void processPayment_alreadyPaid_throwsException() {
        UUID orderId = UUID.randomUUID();
        String userEmail = "test@example.com";
        BigDecimal amount = new BigDecimal("100.00");
        
        Payment existingPayment = new Payment(UUID.randomUUID(), orderId, 1, "SUCCESS", amount, "USD", "CREDIT_CARD", null, null, OffsetDateTime.now());
        when(paymentRepository.findByIdempotencyKey("new-idempotency-key")).thenReturn(Optional.empty());
        when(paymentRepository.findSuccessfulPaymentByOrderId(orderId)).thenReturn(Optional.of(existingPayment));
        
        PaymentRequest request = new PaymentRequest(orderId, amount, "USD", "CREDIT_CARD");
        
        assertThrows(IllegalStateException.class, () -> {
            paymentService.processPayment(userEmail, "new-idempotency-key", request);
        });
        
        verify(orderService, never()).getOrder(anyString(), any(UUID.class));
        verify(paymentRepository, never()).saveIfAbsent(any(Payment.class));
    }
}
