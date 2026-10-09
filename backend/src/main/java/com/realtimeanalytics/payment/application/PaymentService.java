package com.realtimeanalytics.payment.application;

import com.realtimeanalytics.order.application.OrderService;
import com.realtimeanalytics.payment.api.PaymentRequest;
import com.realtimeanalytics.payment.api.PaymentResponse;
import com.realtimeanalytics.payment.domain.Payment;
import com.realtimeanalytics.payment.domain.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final OrderService orderService;

    public PaymentService(PaymentRepository paymentRepository, OrderService orderService) {
        this.paymentRepository = paymentRepository;
        this.orderService = orderService;
    }

    @Transactional
    public PaymentResponse processPayment(String userEmail, String idempotencyKey, PaymentRequest request) {
        if (idempotencyKey != null) {
            Optional<Payment> existingByIdempotency = paymentRepository.findByIdempotencyKey(idempotencyKey);
            if (existingByIdempotency.isPresent()) {
                Payment p = existingByIdempotency.get();
                if (!p.orderId().equals(request.orderId()) || p.amount().compareTo(request.amount()) != 0) {
                    throw new IllegalArgumentException("Idempotency key already used with different request parameters");
                }
                log.info("Returning existing payment for idempotency key {}", idempotencyKey);
                return new PaymentResponse(p.id(), p.orderId(), p.status(), p.amount(), p.currency(), p.errorCode(), p.createdAt());
            }
        }

        // Idempotency check: if payment already successful, we shouldn't allow another one
        Optional<Payment> existingSuccessfulPayment = paymentRepository.findSuccessfulPaymentByOrderId(request.orderId());
        if (existingSuccessfulPayment.isPresent()) {
            throw new IllegalStateException("Order has already been successfully paid");
        }

        // Validate order and amount
        try {
            orderService.validateOrderForPayment(userEmail, request.orderId(), request.amount());
        } catch (IllegalStateException e) {
            // Another thread might have just completed the payment for this idempotency key
            if (idempotencyKey != null) {
                Optional<Payment> existing = paymentRepository.findByIdempotencyKey(idempotencyKey);
                if (existing.isPresent()) {
                    Payment p = existing.get();
                    if (!p.orderId().equals(request.orderId()) || p.amount().compareTo(request.amount()) != 0) {
                        throw new IllegalArgumentException("Idempotency key used with different request parameters");
                    }
                    return new PaymentResponse(p.id(), p.orderId(), p.status(), p.amount(), p.currency(), p.errorCode(), p.createdAt());
                }
            }
            throw e;
        }

        int attemptNumber = paymentRepository.countByOrderId(request.orderId()) + 1;
        
        // Simulate payment provider interaction (for local testing, we'll assume success unless we introduce a special header/amount to fail)
        boolean paymentSuccess = simulatePaymentGateway(request);
        
        Payment payment = new Payment(
                null,
                request.orderId(),
                attemptNumber,
                paymentSuccess ? "SUCCESS" : "FAILED",
                request.amount(),
                request.currency(),
                request.paymentMethod(),
                paymentSuccess ? null : "DECLINED",
                idempotencyKey,
                null
        );
        
        Optional<Payment> savedOptional;
        try {
            savedOptional = paymentRepository.saveIfAbsent(payment);
        } catch (org.springframework.dao.DataAccessException ex) {
            // Handle edge case where uq_order_attempt is evaluated before uq_payment_idempotency by Postgres,
            // or where pessimistic locking/deadlocks occur due to concurrent ON CONFLICT inserts.
            Optional<Payment> concurrentPayment = paymentRepository.findByIdempotencyKey(idempotencyKey);
            if (concurrentPayment.isPresent()) {
                Payment p = concurrentPayment.get();
                if (!p.orderId().equals(request.orderId()) || p.amount().compareTo(request.amount()) != 0) {
                    throw new IllegalArgumentException("Idempotency key used with different request parameters");
                }
                return new PaymentResponse(p.id(), p.orderId(), p.status(), p.amount(), p.currency(), p.errorCode(), p.createdAt());
            }
            throw ex;
        }

        if (savedOptional.isEmpty()) {
            // Another thread inserted the payment using the same idempotency key concurrently
            Payment p = paymentRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("Duplicate idempotency key detected but payment not found"));
            return new PaymentResponse(p.id(), p.orderId(), p.status(), p.amount(), p.currency(), p.errorCode(), p.createdAt());
        }
        
        Payment savedPayment = savedOptional.get();
        
        if (paymentSuccess) {
            orderService.markOrderAsPaid(request.orderId());
        } else {
            orderService.markOrderAsPaymentFailed(request.orderId());
        }
        
        return new PaymentResponse(
                savedPayment.id(),
                savedPayment.orderId(),
                savedPayment.status(),
                savedPayment.amount(),
                savedPayment.currency(),
                savedPayment.errorCode(),
                savedPayment.createdAt()
        );
    }

    private boolean simulatePaymentGateway(PaymentRequest request) {
        // Simple simulation: fail if payment method is explicitly "FAIL"
        return !"FAIL".equalsIgnoreCase(request.paymentMethod());
    }
}
