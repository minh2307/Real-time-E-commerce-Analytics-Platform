package com.realtimeanalytics.payment.domain;

import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository {
    Payment save(Payment payment);
    Optional<Payment> saveIfAbsent(Payment payment);
    Optional<Payment> findById(UUID id);
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);
    int countByOrderId(UUID orderId);
    Optional<Payment> findSuccessfulPaymentByOrderId(UUID orderId);
}
