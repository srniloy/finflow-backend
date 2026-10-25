package com.finflow.authservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.finflow.authservice.client.AccountServiceClient;
import com.finflow.authservice.dto.AccountCreationRequest;
import com.finflow.authservice.dto.AccountCreationResponse;
import com.finflow.authservice.dto.AuthResponse;
import com.finflow.authservice.dto.LoginRequest;
import com.finflow.authservice.dto.RegisterRequest;
import com.finflow.authservice.entity.User;
import com.finflow.authservice.repository.UserRepository;
import com.finflow.authservice.security.JwtService;
import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;

@Service
@Slf4j
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final AccountServiceClient accountServiceClient;

    /**
     * Registers a user and opens a bank account for them via account-service.
     * The account key (FLOWSAVINGS) is stored so the user can log in later.
     */
    public AuthResponse register(RegisterRequest request) {
        log.info("Registering user: {}", request.getEmail());

        if (userRepository.existsByEmail(request.getEmail())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "An account already exists for email: " + request.getEmail());
        }

        if (userRepository.existsByPhone(request.getPhone())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "An account already exists for this phone number: " + request.getPhone());
        }

        // Create the bank account in account-service.
        AccountCreationResponse account;
        try {
            account = accountServiceClient.createAccount(new AccountCreationRequest(
                    request.getName(),
                    request.getEmail(),
                    request.getPhone(),
                    "SAVINGS",
                    new BigDecimal("10000")
            ));
        } catch (FeignException e) {
            // account-service rejected the request - propagate its status and message
            // (e.g. "Account already exists for email: ...") so the user sees the real reason.
            log.error("account-service error for {}: {} {}", request.getEmail(), e.status(), e.getMessage());
            throw new ResponseStatusException(propagatedStatus(e), propagatedMessage(e));
        } catch (Exception e) {
            log.error("Could not create deposit account for {}: {}", request.getEmail(), e.getMessage());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Could not open a bank account for this email. Please try again later.");
        }

        User user = User.builder()
                .name(request.getName())
                .email(request.getEmail())
                .passwordHash(passwordEncoder.encode(request.getPassword()))
                .phone(request.getPhone())
                .accountNumber(account.getAccountNumber())
                .build();

        User savedUser = userRepository.save(user);
        log.info("User registered: {} with account {}", savedUser.getEmail(), savedUser.getAccountNumber());

        return AuthResponse.builder()
                .message("Registration successful. Please log in.")
                .userId(savedUser.getId())
                .name(savedUser.getName())
                .email(savedUser.getEmail())
                .accountId(savedUser.getAccountNumber())
                .build();
    }

    /**
     * Logs a user in and issues a JWT valid for 60 minutes.
     */
    public AuthResponse login(LoginRequest request) {
        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid email or password"));

        if (!passwordEncoder.matches(request.getPassword(), user.getPasswordHash())) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid email or password");
        }

        String token = jwtService.generateToken(
                user.getId(),
                user.getName(),
                user.getEmail(),
                user.getAccountNumber(),
                user.getRole()
        );
        log.info("JWT issued for user {} (valid 60 minutes)", user.getEmail());

        return AuthResponse.builder()
                .message("Login successful")
                .token(token)
                .userId(user.getId())
                .name(user.getName())
                .email(user.getEmail())
                .accountId(user.getAccountNumber())
                .build();
    }

    /**
     * Maps an account-service failure to an HTTP status for the frontend.
     * A connection failure (status -1/0) means account-service is down -> 503;
     * its own business errors (409 duplicate email, etc.) are propagated verbatim.
     */
    private HttpStatus propagatedStatus(FeignException e) {
        int status = e.status();
        if (status < 400 || status > 599) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        return HttpStatus.valueOf(status);
    }

    /** Reads the `message` field from account-service's JSON error body. */
    private String propagatedMessage(FeignException e) {
        try {
            JsonNode node = new ObjectMapper().readTree(e.contentUTF8());
            if (node.has("message") && !node.get("message").isNull()) {
                return node.get("message").asText();
            }
        } catch (Exception ignored) {
            // fall through to the generic message
        }
        return "Could not open a bank account for this email. Please try again later.";
    }

}
