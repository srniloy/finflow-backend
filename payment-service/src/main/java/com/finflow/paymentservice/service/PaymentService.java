package com.finflow.paymentservice.service;

import com.finflow.paymentservice.dto.CreatePaymentRequest;
import com.finflow.paymentservice.dto.PaymentOrderResponse;
import com.finflow.paymentservice.entity.Payment;
import com.finflow.paymentservice.entity.PaymentStatus;
import com.finflow.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.Jar;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.SpringVersion;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;


    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    @Value("${razorpay.webhook-secret}")
    private String webhookSecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    /**
     * Create razorpay payment order.
     *
     * Flow:
     *  1. Create order in razorpay
     *  2. Save payment record in DB
     *  3. Return order details to frontend
     *  4. Frontend show Razorpay Checkout
     *  5. User pays
     *  6. Razorpay calls webhook
     * @param request
     * @return
     */

    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order for account: {} amount: {}",
                request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        // Converted Amount

        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "USD");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace("-", "").substring(0, 10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}", razorpayOrder.get("id").toString());

        // Save payment record

        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                "USD",
                "CREATED",
                keyId
        );
    }

    /**
     * Handles a Razorpay webhook.
     * Returns false when the signature check fails so the controller can answer 400.
     *
     * Flow: Razorpay sends event -> HMAC signature verified against
     * RAZORPAY_WEBHOOK_SECRET -> payment record updated -> Kafka event ->
     * account-service credits the account.
     */
    public boolean handleWebhook(String rawBody, String signature){
        if (!isSignatureValid(rawBody, signature)) {
            log.warn("Rejected Razorpay webhook with invalid/missing X-Razorpay-Signature");
            return false;
        }

        Map<String, Object> payload;
        try {
            payload = objectMapper.readValue(rawBody, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.error("Could not parse Razorpay webhook body: {}", e.getMessage());
            return true; // signature valid but malformed body - acknowledge, don't retry
        }

        String event = (String) payload.get("event");

        log.info("Received Razorpay webhook: {}", event);

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }
        return true;
    }

    /** HMAC-SHA256 of the raw body with the webhook secret must equal X-Razorpay-Signature. */
    private boolean isSignatureValid(String rawBody, String signature){
        if (signature == null || signature.isBlank()
                || webhookSecret == null || webhookSecret.isBlank()) {
            return false;
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(webhookSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(rawBody.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return MessageDigest.isEqual(
                    hex.toString().getBytes(StandardCharsets.UTF_8),
                    signature.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Webhook signature computation failed: {}", e.getMessage());
            return false;
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload){
        try{
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()-> new RuntimeException(
                            "Payment not found for order: "+orderId
                    ));

            // Idempotency - Razorpay retries webhooks; never credit twice.
            if (payment.getStatus() == PaymentStatus.COMPLETED) {
                log.info("Payment {} already completed - ignoring duplicate webhook", payment.getId());
                return;
            }

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);

            paymentRepository.save(payment);

            // Publish payment completed event

            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);
            event.put("description", payment.getDescription());

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);

            log.info("Payment Completed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error handling payment success: {}", e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload){
        try{
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()-> new RuntimeException(
                            "Payment not found for order: "+orderId
                    ));
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", "Payment failed via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);

            log.warn("Payment failed: {}", payment.getId());
        } catch (Exception e) {
            log.error("Error handling payment failure: {}", e.getMessage());
        }
    }

    /**
     * Razorpay webhook structure:
     * { "event": "payment.captured", "payload": { "payment": { "entity": { ... } } } }
     */
    private Map<String, Object> extractPaymentData(Map<String, Object> payload){
        Map<String, Object> payloadWrapper = (Map<String, Object>) payload.get("payload");

        if (payloadWrapper == null) {
            throw new IllegalStateException("Webhook payload missing 'payload' section");
        }

        // entities may arrive keyed "payment" (typical) or "payment.entity" wrappers
        Map<String, Object> paymentWrapper = (Map<String, Object>) payloadWrapper.get("payment");
        if (paymentWrapper == null) {
            paymentWrapper = (Map<String, Object>) payloadWrapper.get("order");
        }
        if (paymentWrapper == null) {
            throw new IllegalStateException(
                    "Webhook payload missing 'payment' entity section");
        }

        return (Map<String, Object>) paymentWrapper.get("entity");
    }

}
