package com.realtimeanalytics.security.jwt;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;

class JwtServiceTest {

    @Test
    void init_FailsWhenSecretTooShort() {
        JwtService jwtService = new JwtService();
        ReflectionTestUtils.setField(jwtService, "secretKey", "short");
        ReflectionTestUtils.setField(jwtService, "jwtExpiration", 3600000L);

        assertThrows(IllegalArgumentException.class, jwtService::validateSecretKey);
    }

    @Test
    void init_SucceedsWhenSecretValid() {
        JwtService jwtService = new JwtService();
        ReflectionTestUtils.setField(jwtService, "secretKey", "aVeryLongTestSecretKeyForHmacSha256ThatMeetsTheLengthRequirement");
        ReflectionTestUtils.setField(jwtService, "jwtExpiration", 3600000L);

        // Should not throw
        jwtService.validateSecretKey();
    }
}
