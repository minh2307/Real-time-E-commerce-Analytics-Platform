package com.realtimeanalytics.user.application;

import com.realtimeanalytics.user.api.AuthResponse;
import com.realtimeanalytics.user.api.LoginRequest;
import com.realtimeanalytics.user.api.RegisterRequest;
import com.realtimeanalytics.user.api.UserResponse;
import com.realtimeanalytics.user.domain.User;
import com.realtimeanalytics.user.domain.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AuthServiceTest {

    private UserRepository userRepository;
    private PasswordEncoder passwordEncoder;
    private TokenService tokenService;
    private AuthService authService;

    @BeforeEach
    void setUp() {
        userRepository = mock(UserRepository.class);
        passwordEncoder = mock(PasswordEncoder.class);
        tokenService = mock(TokenService.class);
        authService = new AuthService(userRepository, passwordEncoder, tokenService);
    }

    @Test
    void register_Success() {
        RegisterRequest request = new RegisterRequest("test@example.com", "password123", "First", "Last");
        
        when(userRepository.existsByEmail("test@example.com")).thenReturn(false);
        when(passwordEncoder.encode("password123")).thenReturn("hashedPassword");
        
        UserResponse response = authService.register(request);
        
        assertNotNull(response);
        assertEquals("test@example.com", response.email());
        assertEquals("First", response.firstName());
        assertEquals("Last", response.lastName());
        
        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(userCaptor.capture());
        
        User savedUser = userCaptor.getValue();
        assertEquals("test@example.com", savedUser.email());
        assertEquals("hashedPassword", savedUser.passwordHash());
    }

    @Test
    void register_DuplicateEmail_ThrowsException() {
        RegisterRequest request = new RegisterRequest("test@example.com", "password123", "First", "Last");
        
        when(userRepository.existsByEmail("test@example.com")).thenReturn(true);
        
        assertThrows(DuplicateEmailException.class, () -> authService.register(request));
        verify(userRepository, never()).save(any());
    }

    @Test
    void login_Success() {
        LoginRequest request = new LoginRequest("test@example.com", "password123");
        User user = User.create("test@example.com", "hashedPassword", "First", "Last");
        
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(user));
        when(passwordEncoder.matches("password123", "hashedPassword")).thenReturn(true);
        when(tokenService.generateToken(user)).thenReturn("jwtToken");
        
        AuthResponse response = authService.login(request);
        
        assertEquals("jwtToken", response.token());
    }

    @Test
    void login_InvalidPassword_ThrowsException() {
        LoginRequest request = new LoginRequest("test@example.com", "wrongPassword");
        User user = User.create("test@example.com", "hashedPassword", "First", "Last");
        
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(user));
        when(passwordEncoder.matches("wrongPassword", "hashedPassword")).thenReturn(false);
        
        assertThrows(InvalidCredentialsException.class, () -> authService.login(request));
    }

    @Test
    void login_UnknownUser_ThrowsException() {
        LoginRequest request = new LoginRequest("test@example.com", "password123");
        
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.empty());
        
        assertThrows(InvalidCredentialsException.class, () -> authService.login(request));
    }
}
