package com.realtimeanalytics.cart.infrastructure;

import com.realtimeanalytics.cart.domain.Cart;
import com.realtimeanalytics.cart.domain.CartRepository;
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
public class JdbcCartRepository implements CartRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcCartRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<Cart> rowMapper = (rs, rowNum) -> new Cart(
            rs.getObject("id", UUID.class),
            rs.getObject("user_id", UUID.class),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    @Override
    public void save(Cart cart) {
        String sql = """
            INSERT INTO carts (id, user_id, created_at, updated_at)
            VALUES (:id, :userId, :createdAt, :updatedAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", cart.id())
                .addValue("userId", cart.userId())
                .addValue("createdAt", Timestamp.from(cart.createdAt()))
                .addValue("updatedAt", Timestamp.from(cart.updatedAt()));
        jdbcTemplate.update(sql, params);
    }

    @Override
    public Optional<Cart> findByUserId(UUID userId) {
        String sql = "SELECT * FROM carts WHERE user_id = :userId";
        List<Cart> results = jdbcTemplate.query(sql, new MapSqlParameterSource("userId", userId), rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public void updateTimestamp(UUID cartId) {
        String sql = "UPDATE carts SET updated_at = :updatedAt WHERE id = :cartId";
        jdbcTemplate.update(sql, new MapSqlParameterSource()
                .addValue("updatedAt", Timestamp.from(Instant.now()))
                .addValue("cartId", cartId));
    }

    @Override
    public void deleteByUserId(UUID userId) {
        String sql = "DELETE FROM carts WHERE user_id = :userId";
        jdbcTemplate.update(sql, new MapSqlParameterSource("userId", userId));
    }
}
