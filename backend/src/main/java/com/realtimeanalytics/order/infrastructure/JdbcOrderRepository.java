package com.realtimeanalytics.order.infrastructure;

import com.realtimeanalytics.order.domain.Order;
import com.realtimeanalytics.order.domain.OrderItem;
import com.realtimeanalytics.order.domain.OrderRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcOrderRepository implements OrderRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcOrderRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<Order> orderRowMapper = (rs, rowNum) -> new Order(
            rs.getObject("id", UUID.class),
            rs.getObject("customer_id", UUID.class),
            rs.getString("status"),
            rs.getBigDecimal("subtotal"),
            rs.getBigDecimal("tax_amount"),
            rs.getBigDecimal("shipping_amount"),
            rs.getBigDecimal("total_amount"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    private final RowMapper<OrderItem> itemRowMapper = (rs, rowNum) -> new OrderItem(
            rs.getObject("id", UUID.class),
            rs.getObject("order_id", UUID.class),
            rs.getObject("product_id", UUID.class),
            rs.getString("product_sku"),
            rs.getString("product_title"),
            rs.getBigDecimal("unit_price"),
            rs.getInt("quantity"),
            rs.getBigDecimal("line_total")
    );

    @Override
    public void save(Order order, List<OrderItem> items) {
        String insertOrderSql = """
            INSERT INTO orders (id, customer_id, status, subtotal, tax_amount, shipping_amount, total_amount, created_at, updated_at)
            VALUES (:id, :customerId, :status, :subtotal, :taxAmount, :shippingAmount, :totalAmount, :createdAt, :updatedAt)
            """;
            
        MapSqlParameterSource orderParams = new MapSqlParameterSource()
                .addValue("id", order.id())
                .addValue("customerId", order.customerId())
                .addValue("status", order.status())
                .addValue("subtotal", order.subtotal())
                .addValue("taxAmount", order.taxAmount())
                .addValue("shippingAmount", order.shippingAmount())
                .addValue("totalAmount", order.totalAmount())
                .addValue("createdAt", Timestamp.from(order.createdAt()))
                .addValue("updatedAt", Timestamp.from(order.updatedAt()));
                
        jdbcTemplate.update(insertOrderSql, orderParams);
        
        String insertItemSql = """
            INSERT INTO order_items (id, order_id, product_id, product_sku, product_title, unit_price, quantity, line_total)
            VALUES (:id, :orderId, :productId, :productSku, :productTitle, :unitPrice, :quantity, :lineTotal)
            """;
            
        MapSqlParameterSource[] itemParams = items.stream().map(item -> new MapSqlParameterSource()
                .addValue("id", item.id())
                .addValue("orderId", item.orderId())
                .addValue("productId", item.productId())
                .addValue("productSku", item.productSku())
                .addValue("productTitle", item.productTitle())
                .addValue("unitPrice", item.unitPrice())
                .addValue("quantity", item.quantity())
                .addValue("lineTotal", item.lineTotal())
        ).toArray(MapSqlParameterSource[]::new);
        
        jdbcTemplate.batchUpdate(insertItemSql, itemParams);
    }

    @Override
    public Optional<Order> findById(UUID id) {
        String sql = "SELECT * FROM orders WHERE id = :id";
        List<Order> results = jdbcTemplate.query(sql, new MapSqlParameterSource("id", id), orderRowMapper);
        return results.stream().findFirst();
    }

    @Override
    public List<OrderItem> findItemsByOrderId(UUID orderId) {
        String sql = "SELECT * FROM order_items WHERE order_id = :orderId";
        return jdbcTemplate.query(sql, new MapSqlParameterSource("orderId", orderId), itemRowMapper);
    }

    @Override
    public List<Order> findByCustomerId(UUID customerId, int limit, int offset) {
        String sql = "SELECT * FROM orders WHERE customer_id = :customerId ORDER BY created_at DESC LIMIT :limit OFFSET :offset";
        return jdbcTemplate.query(sql, new MapSqlParameterSource()
                .addValue("customerId", customerId)
                .addValue("limit", limit)
                .addValue("offset", offset), orderRowMapper);
    }

    @Override
    public void updateStatus(UUID orderId, String status) {
        String sql = "UPDATE orders SET status = :status, updated_at = :updatedAt WHERE id = :orderId";
        jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("status", status)
                .addValue("updatedAt", Timestamp.from(Instant.now()))
                .addValue("orderId", orderId));
    }
}
