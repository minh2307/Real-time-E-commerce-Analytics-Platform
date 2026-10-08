package com.realtimeanalytics.user.infrastructure;

import com.realtimeanalytics.user.domain.User;
import com.realtimeanalytics.user.domain.UserRepository;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcUserRepository implements UserRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcUserRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void save(User user) {
        String sql = """
            INSERT INTO users (id, email, password_hash, first_name, last_name, created_at, updated_at)
            VALUES (:id, :email, :password_hash, :first_name, :last_name, :created_at, :updated_at)
            """;
        
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", user.id())
            .addValue("email", user.email())
            .addValue("password_hash", user.passwordHash())
            .addValue("first_name", user.firstName())
            .addValue("last_name", user.lastName())
            .addValue("created_at", user.createdAt())
            .addValue("updated_at", user.updatedAt());
            
        jdbcTemplate.update(sql, params);
    }

    @Override
    public Optional<User> findByEmail(String email) {
        String sql = "SELECT * FROM users WHERE email = :email";
        try {
            User user = jdbcTemplate.queryForObject(sql, new MapSqlParameterSource("email", email), this::mapRow);
            return Optional.ofNullable(user);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean existsByEmail(String email) {
        String sql = "SELECT COUNT(1) FROM users WHERE email = :email";
        Integer count = jdbcTemplate.queryForObject(sql, new MapSqlParameterSource("email", email), Integer.class);
        return count != null && count > 0;
    }

    private User mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new User(
            rs.getObject("id", UUID.class),
            rs.getString("email"),
            rs.getString("password_hash"),
            rs.getString("first_name"),
            rs.getString("last_name"),
            "USER", // Hardcoded role since it's not in DB schema V1
            rs.getObject("created_at", OffsetDateTime.class),
            rs.getObject("updated_at", OffsetDateTime.class)
        );
    }
}
