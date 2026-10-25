package com.finflow.authservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response body for /register and /login.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AuthResponse {

    private String message;

    private String token;       // JWT (only present on /login)

    private String userId;

    private String name;

    private String email;

    // The bank account number created for this user (also included in the JWT).
    private String accountId;

}
