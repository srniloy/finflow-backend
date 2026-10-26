package com.finflow.paymentservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot 4 auto-configures a Jackson 3 {@code tools.jackson.databind.ObjectMapper}
 * and no longer exposes a Jackson 2 {@code com.fasterxml.jackson.databind.ObjectMapper}
 * bean. The Razorpay SDK is Jackson 2 based, so we register one explicitly for
 * webhook payload parsing in PaymentService.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
