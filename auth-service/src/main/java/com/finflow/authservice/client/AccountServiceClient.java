package com.finflow.authservice.client;

import com.finflow.authservice.dto.AccountCreationRequest;
import com.finflow.authservice.dto.AccountCreationResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    /**
     * Creates a bank account in account-service when a user registers.
     * Same endpoint account-service exposes for internal/at-rest account creation.
     */
    @PostMapping("/api/v1/accounts")
    AccountCreationResponse createAccount(@RequestBody AccountCreationRequest request);

}
