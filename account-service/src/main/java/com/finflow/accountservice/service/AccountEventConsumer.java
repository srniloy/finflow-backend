package com.finflow.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    /**
     * Consume transaction.completed event from kafka
     * Credits receiver account
     * @param payload
     */

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
            ){
        try {
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} amount: {}", receiverAccountNumber, amount);

            accountService.creditBalance(receiverAccountNumber, amount);
        }
        catch (Exception e){
            log.error("Error crediting account: {}", e.getMessage());
        }
    }

    /**
     * Consume payment.completed event from kafka (Razorpay webhook -> payment-service).
     * Credits the account balance for the payment.
     * @param payload
     */

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload
            ){
        try {
            String accountNumber = (String) payload.get("accountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Payment completed - crediting account: {} amount: {}", accountNumber, amount);

            accountService.creditBalance(accountNumber, amount);
        }
        catch (Exception e){
            log.error("Error crediting account for payment: {}", e.getMessage());
        }
    }

    /**
     * Consume fraud.detected event from kafka
     * Blocks the flagged account.
     * @param payload
     */

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected
            (
                    @Payload Map<String, Object> payload
            ){
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - blocking account: {}", accountNumber);

            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error blocking account: {}", e.getMessage());
        }

    }
}

