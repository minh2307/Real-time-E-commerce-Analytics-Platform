package com.realtimeanalytics.user.application;

import com.realtimeanalytics.user.domain.User;

public interface TokenService {
    String generateToken(User user);
}
