package com.finflow.transactionservice.service;

import com.finflow.transactionservice.client.AccountServiceClient;
import com.finflow.transactionservice.dto.AmountRequest;
import com.finflow.transactionservice.dto.TransactionResponse;
import com.finflow.transactionservice.dto.TransferRequest;
import com.finflow.transactionservice.entity.Transaction;
import com.finflow.transactionservice.entity.TransactionStatus;
import com.finflow.transactionservice.entity.TransactionType;
import com.finflow.transactionservice.event.TransactionCompletedEvent;
import com.finflow.transactionservice.event.TransactionInitiatedEvent;
import com.finflow.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cglib.core.Local;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    /** OTP Redis TTL is 5 min - sweep anything older than this. */
    private static final int OTP_EXPIRY_GRACE_MINUTES = 6;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTION_TOPIC = "fraud.detected";
    // separate topics: transaction.completed also credits the receiver via
    // account-service - deposit/withdrawal already moved the money synchronously
    private static final String TRANSACTION_DEPOSIT_TOPIC = "transaction.deposit.completed";
    private static final String TRANSACTION_WITHDRAWAL_TOPIC = "transaction.withdrawal.completed";

    /**
     * SAGA STEP - 1: Initiate transfer
     * Deducts from sender via feign
     * Saves transaction as PROCESSING.
     * Publish event to kafka for fraud check
     * Returns.
     * @param request
     * @return
     */

    public TransactionResponse transfer(TransferRequest request){
        log.info("SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // SAGA STEP 1: Deduct from sender
        accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount()
        );
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent published: {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);

    }

    /**
     * Deposits money into an account.
     * Credits the account synchronously via the existing Feign client (credit endpoint),
     * then saves the transaction as COMPLETED.
     */
    public TransactionResponse deposit(
            AmountRequest request
    ){
        log.info("Deposit: {} amount: {}", request.getAccountNumber(), request.getAmount());

        // Credit the receiver account (money in).
        accountServiceClient.creditBalance(request.getAccountNumber(), request.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getAccountNumber());  // external source -> self
        transaction.setReceiverAccountNumber(request.getAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.DEPOSIT);
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Deposit completed: {}", savedTransaction.getId());

        publishCompletedEvent(TRANSACTION_DEPOSIT_TOPIC, savedTransaction);

        return mapToResponse(savedTransaction);
    }

    /**
     * Withdraws money from an account.
     * Deducts synchronously via the existing Feign client (deduct endpoint - validates
     * sufficient funds and account status), then saves the transaction as COMPLETED.
     */
    public TransactionResponse withdrawal(
            AmountRequest request
    ){
        log.info("Withdrawal: {} amount: {}", request.getAccountNumber(), request.getAmount());

        // Deduct from the sender account (money out).
        accountServiceClient.deductBalance(request.getAccountNumber(), request.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getAccountNumber());
        transaction.setReceiverAccountNumber(request.getAccountNumber());  // cash -> self
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.WITHDRAWAL);
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Withdrawal completed: {}", savedTransaction.getId());

        publishCompletedEvent(TRANSACTION_WITHDRAWAL_TOPIC, savedTransaction);

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId){        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(()-> new RuntimeException(
                        "Transaction not found: "+transactionId
                ))
        );
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber){
        return transactionRepository
                .findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc(
                        accountNumber, accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(
            String transactionId,
            String otp
    ){
        log.info("OTP verification for the transaction: {}", transactionId);

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()-> new RuntimeException(
                        "Transaction not found "+ transactionId
                ));

        String otpKey = "verification:otp"+ transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);


        if(storedOtp == null){
            // OTP EXPIRED
            log.warn("OTP expired for transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            // BLOCK ACCOUNT AND REFUND
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            blockAccountAndCompensate(transaction,
                    "Wrong OTP entered - transaction cancelled, "+
                    "account blocked for security");

            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction
        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason){
        // Guard against double-refund: the scheduled sweeper and a manual verifyOTP
        // can race - only compensate a transaction that is still in flight.
        if (transaction.getStatus() != TransactionStatus.PROCESSING
                && transaction.getStatus() != TransactionStatus.PENDING_VERIFICATION) {
            log.warn("Skipping compensation - transaction {} already in status {}",
                    transaction.getId(), transaction.getStatus());
            return;
        }

        log.warn("SAGA COMPENSATION - refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());
        // CREDIT MONEY BACK TO THE SENDER SYNCHRONOUSLY

        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount()
        );

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                " - SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());

        transactionRepository.save(transaction);

        // PUBLISH refund event - Notification service will alert user

        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}",
                transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason){
        // Publish fraud.detected -> Account Service will block account

        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTION_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);

        log.warn("fraud.detected published - account: {} will be blocked, kindly contant to the bank",
                transaction.getSenderAccountNumber());


        // SAGA COMPENSATION - refund Sender
        compensateTransaction(transaction, reason);
    }

    /** Publishes a completed-transaction event (deposit / withdrawal notifications). */
    private void publishCompletedEvent(String topic, Transaction transaction){
        TransactionCompletedEvent event = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );
        kafkaTemplate.send(topic, transaction.getId(), event);
        log.info("Published {}: {}", topic, transaction.getId());
    }

    private void completeTransaction(Transaction transaction){        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

        log.info("SAGA COMPLETE - Transaction {} completed",
                transaction.getId());
    }

    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()-> new RuntimeException(
                        "Transaction not found "+ transactionId
                ));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING - skipping", transactionId);
            return;
        }

        completeTransaction(transaction);
    }

    /**
     * Auto-expiry sweeper: transactions stuck in PENDING_VERIFICATION after the
     * OTP has expired (Redis TTL is 5 min) are cancelled and refunded, so an
     * ignored OTP cannot freeze the deducted amount forever.
     */
    @Scheduled(fixedDelay = 60_000)
    public void expireStalePendingVerifications(){
        List<Transaction> stale = transactionRepository.findByStatusAndCreatedAtBefore(
                TransactionStatus.PENDING_VERIFICATION,
                LocalDateTime.now().minusMinutes(OTP_EXPIRY_GRACE_MINUTES));

        for (Transaction transaction : stale) {
            // skip if the OTP somehow still exists - never refund a live verification
            String otpKey = "verification:otp" + transaction.getId();
            if (redisTemplate.opsForValue().get(otpKey) != null) {
                continue;
            }
            log.warn("Auto-expiring stale PENDING_VERIFICATION transaction: {}", transaction.getId());
            compensateTransaction(transaction,
                    "OTP expired - transaction cancelled and amount refunded");
        }
    }

    private TransactionResponse mapToResponse(Transaction transaction){
        TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
        response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setFailureReason(transaction.getFailureReason());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompletedAt(transaction.getCompletedAt());

        return response;
    }


}
