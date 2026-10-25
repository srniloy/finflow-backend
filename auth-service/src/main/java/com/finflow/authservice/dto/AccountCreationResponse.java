package com.finflow.authservice.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Subset of account-service AccountResponse returned by POST /api/v1/accounts.
 */
@Data
@NoArgsConstructor
public class AccountCreationResponse {

    private String id;

    private String accountNumber;

    private String accountHolderName;

    private String email;

}
