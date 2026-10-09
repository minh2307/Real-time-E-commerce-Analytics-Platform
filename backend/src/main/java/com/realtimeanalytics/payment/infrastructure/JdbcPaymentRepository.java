package com.realtimeanalytics.payment.infrastructure;

import com.realtimeanalytics.payment.domain.Payment;
import com.realtimeanalytics.payment.domain.PaymentRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcPaymentRepository implements PaymentRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcPaymentRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<Payment> rowMapper = (rs, rowNum) -> {
        Timestamp createdAtTs = rs.getTimestamp("created_at");
        return new Payment(
                rs.getObject("id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getInt("attempt_number"),
                rs.getString("status"),
                rs.getBigDecimal("amount"),
                rs.getString("currency"),
                rs.getString("payment_method"),
                rs.getString("error_code"),
                rs.getString("idempotency_key"),
                createdAtTs != null ? createdAtTs.toInstant().atOffset(ZoneOffset.UTC) : null
        );
    };

    @Override
    public Payment save(Payment payment) {
        String sql = """
            INSERT INTO payments (id, order_id, attempt_number, status, amount, currency, payment_method, error_code, idempotency_key)
            VALUES (gen_random_uuid(), :orderId, :attemptNumber, :status, :amount, :currency, :paymentMethod, :errorCode, :idempotencyKey)
            RETURNING *
            """;

        MapSqlParameterSource params = createParams(payment);
        return jdbcTemplate.queryForObject(sql, params, rowMapper);
    }

    @Override
    @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NESTED)
    public Optional<Payment> saveIfAbsent(Payment payment) {
        if (payment.idempotencyKey() == null) {
            return Optional.of(save(payment));
        }
        
        String sql = """
            INSERT INTO payments (id, order_id, attempt_number, status, amount, currency, payment_method, error_code, idempotency_key)
            VALUES (gen_random_uuid(), :orderId, :attemptNumber, :status, :amount, :currency, :paymentMethod, :errorCode, :idempotencyKey)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING *
            """;

        MapSqlParameterSource params = createParams(payment);
        List<Payment> results = jdbcTemplate.query(sql, params, rowMapper);
        return results.stream().findFirst();
    }

    private MapSqlParameterSource createParams(Payment payment) {
        return new MapSqlParameterSource()
                .addValue("orderId", payment.orderId())
                .addValue("attemptNumber", payment.attemptNumber())
                .addValue("status", payment.status())
                .addValue("amount", payment.amount())
                .addValue("currency", payment.currency())
                .addValue("paymentMethod", payment.paymentMethod())
                .addValue("errorCode", payment.errorCode())
                .addValue("idempotencyKey", payment.idempotencyKey());
    }

    @Override
    public Optional<Payment> findById(UUID id) {
        String sql = "SELECT * FROM payments WHERE id = :id";
        List<Payment> results = jdbcTemplate.query(sql, new MapSqlParameterSource("id", id), rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public Optional<Payment> findByIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null) return Optional.empty();
        String sql = "SELECT * FROM payments WHERE idempotency_key = :idempotencyKey";
        List<Payment> results = jdbcTemplate.query(sql, new MapSqlParameterSource("idempotencyKey", idempotencyKey), rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public int countByOrderId(UUID orderId) {
        String sql = "SELECT COUNT(*) FROM payments WHERE order_id = :orderId";
        Integer count = jdbcTemplate.queryForObject(sql, new MapSqlParameterSource("orderId", orderId), Integer.class);
        return count != null ? count : 0;
    }

    @Override
    public Optional<Payment> findSuccessfulPaymentByOrderId(UUID orderId) {
        String sql = "SELECT * FROM payments WHERE order_id = :orderId AND status = 'SUCCESS' LIMIT 1";
        List<Payment> results = jdbcTemplate.query(sql, new MapSqlParameterSource("orderId", orderId), rowMapper);
        return results.stream().findFirst();
    }
}
