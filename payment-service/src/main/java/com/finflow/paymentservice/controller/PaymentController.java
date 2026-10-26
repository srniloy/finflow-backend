package com.finflow.paymentservice.controller;

import com.finflow.paymentservice.dto.CreatePaymentRequest;
import com.finflow.paymentservice.dto.PaymentOrderResponse;
import com.finflow.paymentservice.entity.PaymentStatus;
import com.finflow.paymentservice.service.PaymentService;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/payments")
@Slf4j
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/create-order")
    public ResponseEntity<PaymentOrderResponse> createPaymentOrder(
            @Valid @RequestBody CreatePaymentRequest request
            ) throws RazorpayException {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(paymentService.createPaymentOrder(request));
    }

    // Razorpay webhook endpoint (public, but signature-verified).
    // Raw body is required because the HMAC signature is computed over the exact bytes.
    @PostMapping("/webhook")
    public ResponseEntity<String> handleWebhook(
            @RequestHeader(value = "X-Razorpay-Signature", required = false) String signature,
            @RequestBody String rawBody
    ){
        if (!paymentService.handleWebhook(rawBody, signature)) {
            return ResponseEntity.badRequest().body("Invalid webhook signature");
        }
        return ResponseEntity.ok("Webhook processed");
    }
}
