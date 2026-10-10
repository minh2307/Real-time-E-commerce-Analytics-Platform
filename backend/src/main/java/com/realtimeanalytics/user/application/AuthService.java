package com.realtimeanalytics.user.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.realtimeanalytics.outbox.application.OutboxService;
import com.realtimeanalytics.user.api.AuthResponse;
import com.realtimeanalytics.user.api.LoginRequest;
import com.realtimeanalytics.user.api.RegisterRequest;
import com.realtimeanalytics.user.api.UserResponse;
import com.realtimeanalytics.user.domain.User;
import com.realtimeanalytics.user.domain.UserRepository;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final TokenService tokenService;
    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    public AuthService(UserRepository userRepository, PasswordEncoder passwordEncoder, TokenService tokenService, OutboxService outboxService, ObjectMapper objectMapper) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.tokenService = tokenService;
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public UserResponse register(RegisterRequest request) {
        if (userRepository.existsByEmail(request.email())) {
            throw new DuplicateEmailException("Email is already taken");
        }

        String encodedPassword = passwordEncoder.encode(request.password());
        User user = User.create(
            request.email(),
            encodedPassword,
            request.firstName(),
            request.lastName()
        );

        userRepository.save(user);

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("userId", user.id().toString());
        payload.put("email", user.email());
        payload.put("username", user.firstName() + "_" + user.lastName());
        payload.put("fullName", user.firstName() + " " + user.lastName());
        payload.put("role", user.role());
        payload.put("status", "ACTIVE");
        outboxService.saveEvent("User", user.id(), "USER_REGISTERED", payload.toString());

        return new UserResponse(
            user.id(),
            user.email(),
            user.firstName(),
            user.lastName(),
            user.role()
        );
    }

    @Transactional(readOnly = true)
    public AuthResponse login(LoginRequest request) {
        User user = userRepository.findByEmail(request.email())
            .orElseThrow(() -> new InvalidCredentialsException("Invalid credentials"));

        if (!passwordEncoder.matches(request.password(), user.passwordHash())) {
            throw new InvalidCredentialsException("Invalid credentials");
        }

        String token = tokenService.generateToken(user);
        return new AuthResponse(token);
    }
}
