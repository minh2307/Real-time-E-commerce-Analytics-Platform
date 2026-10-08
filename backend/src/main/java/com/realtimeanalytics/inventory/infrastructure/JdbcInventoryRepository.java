package com.realtimeanalytics.inventory.infrastructure;

import com.realtimeanalytics.inventory.domain.Inventory;
import com.realtimeanalytics.inventory.domain.InventoryRepository;
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
public class JdbcInventoryRepository implements InventoryRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcInventoryRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private final RowMapper<Inventory> rowMapper = (rs, rowNum) -> new Inventory(
            rs.getObject("product_id", UUID.class),
            rs.getInt("available_quantity"),
            rs.getInt("reserved_quantity"),
            rs.getLong("version"),
            rs.getTimestamp("updated_at").toInstant()
    );

    @Override
    public void save(Inventory inventory) {
        String sql = """
            INSERT INTO inventory (product_id, available_quantity, reserved_quantity, version, updated_at)
            VALUES (:productId, :availableQuantity, :reservedQuantity, :version, :updatedAt)
            """;
            
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("productId", inventory.productId())
                .addValue("availableQuantity", inventory.availableQuantity())
                .addValue("reservedQuantity", inventory.reservedQuantity())
                .addValue("version", inventory.version())
                .addValue("updatedAt", Timestamp.from(inventory.updatedAt()));
                
        jdbcTemplate.update(sql, params);
    }

    @Override
    public Optional<Inventory> findByProductId(UUID productId) {
        String sql = "SELECT * FROM inventory WHERE product_id = :productId";
        List<Inventory> results = jdbcTemplate.query(sql, new MapSqlParameterSource("productId", productId), rowMapper);
        return results.stream().findFirst();
    }

    @Override
    public boolean update(Inventory inventory) {
        String sql = """
            UPDATE inventory 
            SET available_quantity = :availableQuantity, 
                reserved_quantity = :reservedQuantity, 
                version = version + 1, 
                updated_at = :updatedAt
            WHERE product_id = :productId AND version = :version
            """;
            
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("availableQuantity", inventory.availableQuantity())
                .addValue("reservedQuantity", inventory.reservedQuantity())
                .addValue("updatedAt", Timestamp.from(Instant.now()))
                .addValue("productId", inventory.productId())
                .addValue("version", inventory.version());
                
        int updatedRows = jdbcTemplate.update(sql, params);
        return updatedRows > 0;
    }
}
