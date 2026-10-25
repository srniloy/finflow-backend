package com.finflow.transactionservice.repository;

import com.finflow.transactionservice.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;


public interface TransactionRepository extends JpaRepository<Transaction, String> {

    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);

    /** Sent + received transactions (history fix #3.3). */
    List<Transaction> findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc(
            String senderAccountNumber, String receiverAccountNumber);
}
