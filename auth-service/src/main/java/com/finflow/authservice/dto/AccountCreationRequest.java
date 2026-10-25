package com.finflow.authservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Mirrors account-service CreateAccountRequest (no code duplication in account-service).
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AccountCreationRequest {

    @NotBlank
    private String accountHolderName;

    @NotBlank
    private String email;

    @NotBlank
    private String phone;

    @NotNull
    // Serialized as the enum name (e.g. "SAVINGS") and deserialized by account-service's AccountType.
    private String accountType;

    @NotNull
    @Positive
    private BigDecimal initialDeposit;

}
