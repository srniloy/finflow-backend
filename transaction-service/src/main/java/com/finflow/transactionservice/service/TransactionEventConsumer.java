package com.finflow.transactionservice.service;

import com.finflow.transactionservice.entity.Transaction;
import com.finflow.transactionservice.entity.TransactionStatus;
import com.finflow.transactionservice.entity.TransactionType;
import com.finflow.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private static final long OTP_EXPIRY_MINUTES = 5;
    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";
    private final TransactionService transactionService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * Consume verification.required
     * Generate OTP and ask user to verify
     * @param payload
     */
    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(
            @Payload Map<String, Object> payload
            ){

        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transaction: {} reason: {}",
                    transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(()-> new RuntimeException(
                            "Transaction not found "+ transactionId
                    ));

            // check status to prevent idempotency

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction {} not PROCESSING - skipping", transactionId);
                return;
            }

            // Generate 6 digit opt

            String otp = String.format("%06d", (int) (Math.random() * 900000)+100000);

            String otpKey = "verification:otp"+transactionId;

            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            // Update Status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction: {} expires in {} min",
                    transactionId, OTP_EXPIRY_MINUTES);

            // Notify user

            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);

        } catch (Exception e) {
            log.error("Error handling verification required: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckResult(
            @Payload Map<String, Object> payload
    ){
        try{
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);
        } catch (Exception e) {
            log.error("Error processing fraud check result: {}", e.getMessage());
        }
    }

    /**
     * Consume payment.completed (Razorpay webhook -> payment-service) and record
     * the online payment in the transaction history so it shows in activity lists.
     * The balance credit happens in account-service; here we only add the history record.
     */
    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload
    ){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
            String razorpayPaymentId = (String) payload.get("razorpayPaymentId");
            String description = (String) payload.get("description");

            if (razorpayPaymentId != null &&
                    transactionRepository.existsByReferenceNumber(razorpayPaymentId)) {
                log.info("Payment history record already exists for Razorpay payment {}",
                        razorpayPaymentId);
                return;
            }

            Transaction transaction = new Transaction();
            // paid from an external card via Razorpay - recorded as a credit to the holder
            transaction.setSenderAccountNumber(accountNumber);
            transaction.setReceiverAccountNumber(accountNumber);
            transaction.setAmount(amount);
            transaction.setType(TransactionType.PAYMENT);
            transaction.setStatus(TransactionStatus.COMPLETED);
            transaction.setDescription(description != null && !description.isBlank()
                    ? description
                    : "Online payment via Razorpay");
            transaction.setReferenceNumber(razorpayPaymentId != null
                    ? razorpayPaymentId
                    : UUID.randomUUID().toString());
            transaction.setCompletedAt(LocalDateTime.now());

            Transaction savedTransaction = transactionRepository.save(transaction);
            log.info("Payment recorded in transaction history: {}", savedTransaction.getId());
        } catch (Exception e) {
            log.error("Error recording payment history record: {}", e.getMessage());
        }
    }
}
