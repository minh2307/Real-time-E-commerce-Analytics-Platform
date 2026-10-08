package com.realtimeanalytics.user.domain;

import java.util.Optional;

public interface UserRepository {
    void save(User user);
    Optional<User> findByEmail(String email);
    boolean existsByEmail(String email);
}
