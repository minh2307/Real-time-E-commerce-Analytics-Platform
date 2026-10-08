package com.realtimeanalytics.catalog.infrastructure;

import com.realtimeanalytics.catalog.domain.Product;
import com.realtimeanalytics.catalog.domain.ProductRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcProductRepository implements ProductRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcProductRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<Product> rowMapper = (rs, rowNum) -> new Product(
            rs.getObject("id", UUID.class),
            rs.getObject("category_id", UUID.class),
            rs.getString("sku"),
            rs.getString("title"),
            rs.getString("description"),
            rs.getBigDecimal("price"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    @Override
    public void save(Product product) {
        String sql = """
            INSERT INTO products (id, category_id, sku, title, description, price, created_at, updated_at)
            VALUES (:id, :categoryId, :sku, :title, :description, :price, :createdAt, :updatedAt)
            """;
            
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", product.id())
                .addValue("categoryId", product.categoryId())
                .addValue("sku", product.sku())
                .addValue("title", product.title())
                .addValue("description", product.description())
                .addValue("price", product.price())
                .addValue("createdAt", Timestamp.from(product.createdAt()))
                .addValue("updatedAt", Timestamp.from(product.updatedAt()));
                
        jdbcTemplate.update(sql, params);
    }

    @Override
    public Optional<Product> findById(UUID id) {
        String sql = "SELECT * FROM products WHERE id = :id";
        List<Product> results = jdbcTemplate.query(sql, new MapSqlParameterSource("id", id), rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public List<Product> findByCategoryId(UUID categoryId, int offset, int limit) {
        String sql = "SELECT * FROM products WHERE category_id = :categoryId ORDER BY created_at DESC OFFSET :offset LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("categoryId", categoryId)
                .addValue("offset", offset)
                .addValue("limit", limit);
        return jdbcTemplate.query(sql, params, rowMapper);
    }

    @Override
    public List<Product> findAll(int offset, int limit) {
        String sql = "SELECT * FROM products ORDER BY created_at DESC OFFSET :offset LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("offset", offset)
                .addValue("limit", limit);
        return jdbcTemplate.query(sql, params, rowMapper);
    }
}
