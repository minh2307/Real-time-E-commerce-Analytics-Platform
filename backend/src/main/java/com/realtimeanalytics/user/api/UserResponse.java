package com.realtimeanalytics.user.api;

import java.util.UUID;

public record UserResponse(
    UUID id,
    String email,
    String firstName,
    String lastName,
    String role
) {}
