package com.realtimeanalytics.user.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

public record User(
    UUID id,
    String email,
    String passwordHash,
    String firstName,
    String lastName,
    String role,
    OffsetDateTime createdAt,
    OffsetDateTime updatedAt
) {
    public static User create(String email, String passwordHash, String firstName, String lastName) {
        return new User(
            UUID.randomUUID(),
            email,
            passwordHash,
            firstName,
            lastName,
            "USER", // Default role since V1 doesn't have a role column, we'll hardcode it in domain
            OffsetDateTime.now(),
            OffsetDateTime.now()
        );
    }
}
