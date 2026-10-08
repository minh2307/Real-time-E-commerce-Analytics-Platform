package com.realtimeanalytics.catalog.infrastructure;

import com.realtimeanalytics.catalog.domain.Category;
import com.realtimeanalytics.catalog.domain.CategoryRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcCategoryRepository implements CategoryRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcCategoryRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<Category> rowMapper = (rs, rowNum) -> new Category(
            rs.getObject("id", UUID.class),
            rs.getObject("parent_id", UUID.class),
            rs.getString("slug"),
            rs.getString("name"),
            rs.getString("description"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    @Override
    public void save(Category category) {
        String sql = """
            INSERT INTO categories (id, parent_id, slug, name, description, created_at, updated_at)
            VALUES (:id, :parentId, :slug, :name, :description, :createdAt, :updatedAt)
            """;
        
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", category.id())
                .addValue("parentId", category.parentId())
                .addValue("slug", category.slug())
                .addValue("name", category.name())
                .addValue("description", category.description())
                .addValue("createdAt", Timestamp.from(category.createdAt()))
                .addValue("updatedAt", Timestamp.from(category.updatedAt()));
                
        jdbcTemplate.update(sql, params);
    }

    @Override
    public Optional<Category> findById(UUID id) {
        String sql = "SELECT * FROM categories WHERE id = :id";
        List<Category> results = jdbcTemplate.query(sql, new MapSqlParameterSource("id", id), rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public List<Category> findAll(int offset, int limit) {
        String sql = "SELECT * FROM categories ORDER BY created_at DESC OFFSET :offset LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("offset", offset)
                .addValue("limit", limit);
        return jdbcTemplate.query(sql, params, rowMapper);
    }
}
