package com.finflow.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    private final JavaMailSender mailSender;
    private final RestClient accountServiceClient;
    private final String mailUsername;
    private final String mailFrom;

    public NotificationService(
            JavaMailSender mailSender,
            @Value("${app.account-service.url}") String accountServiceUrl,
            @Value("${spring.mail.username:}") String mailUsername,
            @Value("${app.mail.from:}") String mailFrom) {
        this.mailSender = mailSender;
        this.accountServiceClient = RestClient.builder()
                .baseUrl(accountServiceUrl)
                .build();
        this.mailUsername = mailUsername;
        this.mailFrom = mailFrom;
    }

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOTPGenerated(
            @Payload Map<String, Object> payload
            ){
        try{
            String accountNumber = String.valueOf(payload.get("accountNumber"));
            String otp = String.valueOf(payload.get("otp"));
            String amount = String.valueOf(payload.get("amount"));
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "Transaction verification required",
                    String.format(
                            "A transaction of %s from your account %s is pending verification. "+
                                    "Reason: %s. "+
                                    "Your one-time password (OTP) is %s. It is valid for 5 minutes. "+
                                    "If you did not initiate this transaction, do not verify it and "+
                                    "contact FinFlow support immediately.",
                            amount, accountNumber, reason, otp
                    )
            );

        } catch (Exception e) {
            log.error("Error sending OTP notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.deposit.completed")
    public void consumeDepositCompleted(
            @Payload Map<String, Object> payload
            ){
        try{
            String accountNumber = String.valueOf(payload.get("receiverAccountNumber"));
            String amount = String.valueOf(payload.get("amount"));
            String transactionId = String.valueOf(payload.get("transactionId"));

            sendAlert(accountNumber,
                    "Deposit successful",
                    String.format(
                            "An amount of %s has been credited to your account %s. "+
                                    "Transaction reference: %s.",
                            amount, accountNumber, transactionId
                    ));
        } catch (Exception e) {
            log.error("Error sending deposit notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.withdrawal.completed")
    public void consumeWithdrawalCompleted(
            @Payload Map<String, Object> payload
            ){
        try{
            String accountNumber = String.valueOf(payload.get("senderAccountNumber"));
            String amount = String.valueOf(payload.get("amount"));
            String transactionId = String.valueOf(payload.get("transactionId"));

            sendAlert(accountNumber,
                    "Withdrawal successful",
                    String.format(
                            "An amount of %s has been debited from your account %s. "+
                                    "Transaction reference: %s.",
                            amount, accountNumber, transactionId
                    ));
        } catch (Exception e) {
            log.error("Error sending withdrawal notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
            ){
        try{
            String senderAccount = String.valueOf(payload.get("senderAccountNumber"));
            String receiverAccount = String.valueOf(payload.get("receiverAccountNumber"));
            String amount = String.valueOf(payload.get("amount"));

            // DEBIT ALERT
            sendAlert(senderAccount,
                    "Transfer debit alert",
                    String.format(
                            "An amount of %s has been debited from your account %s "+
                                    "and transferred to account %s.",
                            amount, senderAccount, receiverAccount
                    ));
            // CREDIT ALERT
            sendAlert(receiverAccount,
                    "Transfer credit alert",
                    String.format(
                            "An amount of %s has been credited to your account %s "+
                                    "from account %s.",
                            amount, receiverAccount, senderAccount
                    ));
        } catch (Exception e) {
            log.error("Error sending transaction notification: {}", e.getMessage());
        }
    }
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber, "Account blocked - suspicious activity",
                    String.format(
                            "Your account %s has been blocked due to suspicious activity. "+
                                    "Reason: %s. "+
                                    "Please contact FinFlow support immediately.",
                            accountNumber, reason
                    ));
        } catch (Exception e) {
            log.error("Error sending fraud alert: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(senderAccount,
                    "Transaction cancelled and refunded",
                    String.format(
                            "Your transaction of %s was cancelled and not processed. "+
                                    "Reason: %s. "+
                                    "The amount of %s has been refunded to your account %s.",
                            amount, reason, amount, senderAccount
                    ));
        }
        catch (Exception e) {
            log.error("Error sending refund notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "Payment successful",
                    String.format(
                            "Your payment of %s was completed successfully. "+
                                    "Razorpay payment ID: %s. "+
                                    "The amount has been credited to your account %s.",
                            amount, payload.get("razorpayPaymentId"), accountNumber
                    ));
        }
        catch (Exception e){
            log.error("Error sending payment notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "Payment failed",
                    String.format(
                            "Your payment of %s could not be processed. "+
                                    "No amount has been deducted. "+
                                    "Please try again or contact FinFlow support if the problem persists.",
                            amount
                    ));
        }
        catch (Exception e){
            log.error("Error sending payment failure notification: {}", e.getMessage());
        }
    }


    /**
     * Sends an email to the owner of the given account number.
     * Falls back to log-only when mail is not configured or the
     * email address cannot be resolved - a mail outage must never
     * break the OTP / alert flow.
     */
    private void sendAlert(String accountNumber, String subject, String message){
        AccountContact contact = resolveAccountContact(accountNumber);

        if (contact == null || contact.email() == null || contact.email().isBlank()) {
            log.warn("Could not resolve email for account {} - logging notification only", accountNumber);
            logNotification(accountNumber, subject, message);
            return;
        }

        if (mailUsername == null || mailUsername.isBlank()) {
            log.warn("MAIL_USERNAME not configured - notification for account {} logged only", accountNumber);
            logNotification(accountNumber, subject, message);
            return;
        }

        try {
            SimpleMailMessage mail = new SimpleMailMessage();
            mail.setFrom(mailFrom != null && !mailFrom.isBlank() ? mailFrom : mailUsername);
            mail.setTo(contact.email());
            mail.setSubject("[FinFlow] " + subject);
            mail.setText(buildBody(contact, message));
            mailSender.send(mail);
            log.info("Email sent to {} for account {} - subject: {}",
                    contact.email(), accountNumber, subject);
        } catch (Exception e) {
            log.error("Failed to send email to {} for account {}: {}",
                    contact.email(), accountNumber, e.getMessage());
            // don't lose the OTP / alert just because SMTP failed
            logNotification(accountNumber, subject, message);
        }
    }

    private String buildBody(AccountContact contact, String message){
        String greeting = contact.name() != null && !contact.name().isBlank()
                ? "Hi " + contact.name() + ",\n\n"
                : "Hi,\n\n";
        return greeting
                + message
                + "\n\n- FinFlow Security Team\n"
                + "(This is an automated message, please do not reply.)";
    }

    private AccountContact resolveAccountContact(String accountNumber){
        try {
            Map<String, Object> account = accountServiceClient.get()
                    .uri("/api/v1/accounts/{accountNumber}", accountNumber)
                    .retrieve()
                    .body(new ParameterizedTypeReference<Map<String, Object>>() {});
            if (account == null) {
                return null;
            }
            return new AccountContact(
                    account.get("email") != null ? String.valueOf(account.get("email")) : null,
                    account.get("accountHolderName") != null ? String.valueOf(account.get("accountHolderName")) : null
            );
        } catch (Exception e) {
            log.error("Failed to resolve account contact for {}: {}", accountNumber,
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }
    }

    private void logNotification(String accountNumber, String subject, String message){
        log.info("--------------- Notification Start ------------------------");
        log.info("Account: {}", accountNumber);
        log.info("Subject: {}", subject);
        log.info("Message: {}", message);
        log.info("----------------- Notification End -------------------");
    }

    private record AccountContact(String email, String name) {}
}
