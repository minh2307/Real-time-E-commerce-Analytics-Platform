package com.realtimeanalytics.cart.infrastructure;

import com.realtimeanalytics.cart.domain.CartItem;
import com.realtimeanalytics.cart.domain.CartItemRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcCartItemRepository implements CartItemRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcCartItemRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<CartItem> rowMapper = (rs, rowNum) -> new CartItem(
            rs.getObject("id", UUID.class),
            rs.getObject("cart_id", UUID.class),
            rs.getObject("product_id", UUID.class),
            rs.getInt("quantity"),
            rs.getTimestamp("added_at").toInstant()
    );

    @Override
    public void save(CartItem item) {
        String sql = """
            INSERT INTO cart_items (id, cart_id, product_id, quantity, added_at)
            VALUES (:id, :cartId, :productId, :quantity, :addedAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", item.id())
                .addValue("cartId", item.cartId())
                .addValue("productId", item.productId())
                .addValue("quantity", item.quantity())
                .addValue("addedAt", Timestamp.from(item.addedAt()));
        jdbcTemplate.update(sql, params);
    }

    @Override
    public void updateQuantity(UUID id, int quantity) {
        String sql = "UPDATE cart_items SET quantity = :quantity WHERE id = :id";
        jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("quantity", quantity)
                .addValue("id", id));
    }

    @Override
    public void delete(UUID id) {
        String sql = "DELETE FROM cart_items WHERE id = :id";
        jdbcTemplate.update(sql, new MapSqlParameterSource("id", id));
    }

    @Override
    public void deleteByCartId(UUID cartId) {
        String sql = "DELETE FROM cart_items WHERE cart_id = :cartId";
        jdbcTemplate.update(sql, new MapSqlParameterSource("cartId", cartId));
    }

    @Override
    public Optional<CartItem> findByCartIdAndProductId(UUID cartId, UUID productId) {
        String sql = "SELECT * FROM cart_items WHERE cart_id = :cartId AND product_id = :productId";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("cartId", cartId)
                .addValue("productId", productId);
        List<CartItem> results = jdbcTemplate.query(sql, params, rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public List<CartItem> findByCartId(UUID cartId) {
        String sql = "SELECT * FROM cart_items WHERE cart_id = :cartId ORDER BY added_at ASC";
        return jdbcTemplate.query(sql, new MapSqlParameterSource("cartId", cartId), rowMapper);
    }
}
