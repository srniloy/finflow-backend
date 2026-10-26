package com.finflow.transactionservice.repository;

import com.finflow.transactionservice.entity.Transaction;
import com.finflow.transactionservice.entity.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;


public interface TransactionRepository extends JpaRepository<Transaction, String> {

    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);

    /** Sent + received transactions (history fix #3.3). */
    List<Transaction> findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc(
            String senderAccountNumber, String receiverAccountNumber);

    /** Stale OTP verifications (auto-expiry sweeper). */
    List<Transaction> findByStatusAndCreatedAtBefore(TransactionStatus status, LocalDateTime cutoff);

    /** Idempotency guard for payment history records (reference = Razorpay payment ID). */
    boolean existsByReferenceNumber(String referenceNumber);
}
