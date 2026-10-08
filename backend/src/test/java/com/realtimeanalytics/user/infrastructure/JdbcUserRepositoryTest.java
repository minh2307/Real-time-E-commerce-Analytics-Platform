package com.realtimeanalytics.user.infrastructure;

import com.realtimeanalytics.user.domain.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.jdbc.Sql;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@JdbcTest
@org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase(replace = org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace.NONE)
@Import(JdbcUserRepository.class)
class JdbcUserRepositoryTest {

    @Autowired
    private JdbcUserRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Test
    void saveAndFindByEmail_Success() {
        User user = User.create("test@example.com", "hashedPassword", "First", "Last");
        
        repository.save(user);
        
        Optional<User> found = repository.findByEmail("test@example.com");
        assertTrue(found.isPresent());
        assertEquals(user.id(), found.get().id());
        assertEquals("test@example.com", found.get().email());
        assertEquals("hashedPassword", found.get().passwordHash());
        assertEquals("First", found.get().firstName());
        assertEquals("Last", found.get().lastName());
        assertEquals("USER", found.get().role());
    }

    @Test
    void existsByEmail_ReturnsTrueWhenExists() {
        User user = User.create("existing@example.com", "pass", "A", "B");
        repository.save(user);
        
        assertTrue(repository.existsByEmail("existing@example.com"));
        assertFalse(repository.existsByEmail("nonexisting@example.com"));
    }
}
