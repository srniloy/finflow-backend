# FinFlow Backend — Fixes & New Features

> Purpose: Make the backend demo-ready for a fintech job application.
> Scope: 6 microservices + API Gateway. Each item lists the file, what to change, and why.

---

## Priority Legend

| Tag | Meaning |
|-----|---------|
| **P0** | Must-fix (security / correctness / recruiter red flags) |
| **P1** | Should-add (completes the feature) |
| **P2** | Nice-to-have (polish / best practices) |

---

## 1. Security (P0 — Highest Priority)

### 1.1 Add JWT Authentication at the API Gateway
**File:** `api-gateway/` (new classes)
- Add `spring-boot-starter-security` + `io.jsonwebtoken:jjwt` to `api-gateway/pom.xml`.
- Create `JwtAuthenticationFilter` (extends `OncePerRequestFilter`) that validates `Authorization: Bearer <token>`.
- Create `SecurityConfig` with `SecurityWebFilterChain` (WebFlux) permitting `/api/v1/auth/**` and requiring auth elsewhere.
- Add login/register endpoints. Currently accounts exist without any user identity — consider a minimal `User` entity or accept an account number + PIN.

**Why:** All 15 endpoints are completely open. This is the #1 thing a reviewer will notice.

> **Status: implemented (2026-10-25).** Identity lives in the new **`auth-service`** (port **8086** — 8084/conflict note: the original plan said 8084, but that port belongs to `fraud-detection-service`, Spring Boot 4.1.1/MVC — matches the other services' stack): `User` entity (unique email + BCrypt hash + `ROLE_USER`), `POST /api/v1/auth/login` and `/register` (`register` also opens the user's bank account via Feign `POST /api/v1/accounts` in account-service), structured `{timestamp,status,error,message}` error bodies (incl. duplicate-email `409`). JWTs are issued via jjwt, HS256, claims `sub/uid/name/email/accountNumber/roles`, **60-min TTL** (`auth.jwt.ttl-minutes`, env: `JWT_SECRET` shared dev default in both yamls — see `#1.2`).
> The gateway validates tokens via a global `JwtAuthenticationFilter` (`api-gateway/.../filter/JwtAuthenticationFilter.java`) and a `SecurityConfig` (`api-gateway/.../config/SecurityConfig.java`) — WebFlux has no `OncePerRequestFilter`, so this replaces the planned `JwtAuthenticationFilter`. `/api/v1/auth/**` and `/actuator/**` are public; every other route requires a valid Bearer token (expired tokens → `401`, which is the 60-minute session timeout). CORS for `http://localhost:4200` is configured via `gateway.globalcors` in the gateway yaml (`#6.3` applied).
> Verified end-to-end: register → login → protected-route `401` without token → request forwarded with valid token.

### 1.2 Externalize Secrets
**Files:** all `application.yaml` + `docker-compose.yml`
- Remove hardcoded values from `payment-service/src/main/resources/application.yaml:28-30`:
  ```yaml
  razorpay:
    key-id: ${RAZORPAY_KEY_ID}
    key-secret: ${RAZORPAY_KEY_SECRET}
    webhook-secret: ${RAZORPAY_WEBHOOK_SECRET}
  ```
- Replace DB `root`/`1234` in `account-service`, `transaction-service`, `payment-service` with `${DB_USERNAME}` / `${DB_PASSWORD}`.
- Add `.env.example` (never commit `.env`) and document the variables in README.

### 1.3 Verify Razorpay Webhook Signature
**Files:** `PaymentController.java`, `PaymentService.java`
- Razorpay sends header `X-Razorpay-Signature`. Compute `HMAC-SHA256(webhook-secret, rawBody)` and compare.
- **Problem:** `@RequestBody Map<String,Object>` loses the raw body. Change to `@RequestBody String rawBody` and parse with `ObjectMapper`, or read the raw body from the `ServerHttpRequest` in a filter.
- Reject the webhook (400) if the signature is invalid.

**Why:** Anyone can POST a fake `payment.captured` event and mark a payment complete.

### 1.4 Fix HTTP Verbs for State-Changing Endpoints
**File:** `account-service/.../AccountController.java`
- Change `@GetMapping` → `@PutMapping` (or `@PostMapping`) for:
  - `deductBalance` (`/{accountNumber}/deduct`)
  - `creditBalance` (`/{accountNumber}/credit`)
  - `blockAccount` (`/{accountNumber}/block`)
- Then update the Feign clients in `transaction-service` and `fraud-detection-service` to match.

**Why:** GET must be idempotent/safe. State changes via GET is a well-known anti-pattern.

---

## 2. Payment Service (P0/P1)

### 2.1 Add Payment Status & History Endpoints
**File:** `PaymentController.java` + `PaymentService.java`
```
GET  /api/v1/payments/{paymentId}
GET  /api/v1/payments/account/{accountNumber}
GET  /api/v1/payments/account/{accountNumber}?status=COMPLETED   (filter)
```
- Add `PaymentRepository.findByAccountNumberOrderByCreatedAtDesc`.

### 2.2 Add Refund Endpoint
**File:** `PaymentController.java` + `PaymentService.java`
```
POST /api/v1/payments/{paymentId}/refund
```
- Use `razorpayClient.payments.refund(paymentId, ...)`.
- Set status `REFUNDED` (enum value already exists but is unused).
- Publish a `payment.refunded` Kafka event.

### 2.3 Make RazorpayClient a Singleton Bean
**File:** `PaymentService.java:60`
- Currently `new RazorpayClient(keyId, keySecret)` is created per request.
- Create a `@Configuration` that exposes a `@Bean RazorpayClient` and inject it.

### 2.4 Add @Transactional to createPaymentOrder
**File:** `PaymentService.java:56`
- If `paymentRepository.save()` fails after the Razorpay order is created, an orphan order exists.
- Annotate `createPaymentOrder` with `@Transactional`.

### 2.5 Clean Up
**File:** `PaymentService.java`
- Remove unused imports `org.apache.tomcat.Jar` (line 13) and `org.springframework.core.SpringVersion` (line 16).
- Replace hardcoded `"USD"` (lines 70, 84) with a configurable `currency` property or from the request DTO.

---

## 3. Transaction Service (P0/P1)

### 3.1 Add Deposit & Withdrawal Endpoints — **Status: implemented (2026-10-25).**
**File:** `TransactionController.java` + `TransactionService.java`
```
POST /api/v1/transactions/deposit      { accountNumber, amount, description }   -> TransactionResponse
POST /api/v1/transactions/withdrawal   { accountNumber, amount, description }   -> TransactionResponse
```
- `TransactionType.DEPOSIT` / `WITHDRAWAL` now used.
- Deposit credits the account synchronously via the existing Feign `credit` client; withdrawal deducts via the existing Feign `deduct` client (validates sufficient funds + account status). Transactions are saved directly as `COMPLETED` — `transaction.completed` is NOT published for these, since account-service already credits it via Kafka on that event and would double-credit.

### 3.2 Enforce Daily Transaction Limit
**File:** `TransactionService.java`
- `Account.dailyTransactionLimit` exists but is never checked.
- Track daily total (Redis or a `SUM(amount) WHERE created_at >= today` query) and reject transfers exceeding the limit.

### 3.3 Fix Transaction History (Sent + Received)
**File:** `TransactionRepository.java` + `TransactionService.java`
- Current query `findBySenderAccountNumberOrderByCreatedAtDesc` hides received transactions.
- Add `findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc` and mark each row as `DEBIT`/`CREDIT` in the response.

### 3.4 Add Pagination to History
**File:** `TransactionController.java:40` + `TransactionRepository.java`
- Change the repository method to return `Page<Transaction>`.
- Accept `page` & `size` query params (default 0/20).
- Return a `Page<TransactionResponse>` or a custom paged wrapper.

### 3.5 Add Idempotency & @Transactional
**File:** `TransactionService.java`
- Add an idempotency key (e.g., `Idempotency-Key` header) stored in Redis/DB to prevent duplicate transfers on retries.
- Annotate `transfer()` with `@Transactional` so a DB failure after `deductBalance` rolls back.

### 3.6 Clean Up
- Remove unused imports in `TransferRequest.java` (`jakarta.persistence.Column`) and `TransactionResponse.java` (`jakarta.persistence.*`).
- Fix the event field typo `receiverAccountNUmber` → `receiverAccountNumber` in `TransactionInitiatedEvent.java:15` and `TransactionCompletedEvent.java:15`.

---

## 4. Notification Service (P1)

### 4.1 Send Real Emails
**Files:** `NotificationService.java`, `application.yaml`
- `spring-boot-starter-mail` is already in `pom.xml` but `JavaMailSender` is never used.
- Inject `JavaMailSender` and send from `sendAlert()`.
- Add config:
  ```yaml
  spring:
    mail:
      host: smtp.gmail.com
      port: 587
      username: ${MAIL_USERNAME}
      password: ${MAIL_PASSWORD}
      properties:
        mail.smtp.auth: true
        mail.smtp.starttls.enable: true
  ```
- Use a simple HTML template (Thymeleaf or string builder) instead of inline `String.format`.

### 4.2 Persist Notifications
- Add a `notification` table + entity + repository to store history (user, subject, body, sentAt, status).
- Expose `GET /api/v1/notifications/account/{accountNumber}` (needs a controller — currently none).

### 4.3 Fix Payload Casting
**File:** `NotificationService.java`
- Replace `(String) payload.get("amount")` with `String.valueOf(payload.get("amount"))` (lines 22, 49).
- Kafka deserializes JSON numbers as `Double`/`Integer`, not `String`.

### 4.4 Don't Log OTP in Plain Text
- Avoid logging the OTP value; log only that an OTP was sent.

---

## 5. Fraud Detection Service (P2)

### 5.1 Fix Error Handling
**File:** `FraudDetectionEventConsumer.java:34`
- `throw new RuntimeException(e)` causes infinite Kafka retries with no dead-letter queue.
- Add a `@RetryableTopic` or configure a dead-letter topic; log and skip poison messages.

### 5.2 Fix Payload Field Mapping
**File:** `FraudDetectionService.java`
- Read `senderAccountNumber` (not `accountNumber`) from the `transaction.initiated` event.
- Convert amount safely: `new BigDecimal(String.valueOf(payload.get("amount")))`.

### 5.3 Rename `modal` Package
- `com.finflow.frauddetectionservice.modal` → `model` (typo).

### 5.4 Set TTL on Redis Averages
- `fraud:avg_amount{account}` has no expiry — add a TTL (e.g., 30 days) to avoid stale data.

---

## 6. API Gateway (P0/P2)

### 6.1 Fix Bean Name Typo
**File:** `RateLimiterConfig.java:12`
- `keyResolber()` → `keyResolver()`.

### 6.2 Align Spring Boot Version
- `api-gateway/pom.xml` uses Spring Boot `3.3.0` while all other services use `4.1.1`.
- Upgrade to `4.1.1` (and Spring Cloud `2025.1.3`) for consistency.

### 6.3 Add CORS Configuration
- Add a `WebFilter`/`CorsConfigurationSource` bean so the Angular frontend (e.g., `localhost:4200`) can call the gateway.

### 6.4 Add Circuit Breaker
- Add `spring-cloud-starter-circuitbreaker-reactor-resilience4j` and apply to routes for resilience.

---

## 7. Infrastructure & Best Practices (P1/P2)

### 7.1 Dockerfiles
- Add a `Dockerfile` to each service (multi-stage: Maven build → JRE runtime).
- Add a `docker-compose.override.yml` that includes all 6 services + gateway.

### 7.2 OpenAPI / Swagger
- Add `springdoc-openapi-starter-webmvc-ui` to each service.
- Annotate controllers with `@Operation` / `@Tag`.
- Verify UI at `http://<service>/swagger-ui.html`.

### 7.3 Database Migrations
- Replace `ddl-auto: update` with Flyway/Liquibase for reproducible schema.

### 7.4 Profiles
- Add `application-dev.yaml` / `application-prod.yaml` per service (dev = `show-sql: true`, prod = `false`).

### 7.5 Global Exception Handling
- Add `@RestControllerAdvice` + `@ExceptionHandler` in each service so `RuntimeException` returns a structured error (not a 500 stack trace).

### 7.6 Health Checks
- Add `spring-boot-starter-actuator` readiness/liveness probes for Docker/K8s.

### 7.7 Structured Logging
- Add a request-id/correlation-id (MDC) propagated across services via Kafka headers.

---

## 8. Suggested Implementation Order

1. **P0 Security** — JWT, secret externalization, webhook signature, HTTP verbs
2. **P0 Payments** — status/history, refund, singleton client, `@Transactional`
3. **P0 Transactions** — deposit/withdrawal, daily limit, history fix, pagination
4. **P1 Notifications** — real email, persistence, payload fixes
5. **P1 Infra** — Dockerfiles, OpenAPI, profiles
6. **P2 Polish** — fraud handling, CORS, circuit breaker, migrations, logging

---

## Appendix — Current Endpoint Inventory

| Service | Method | Endpoint |
|---------|--------|----------|
| auth-service | POST | `/api/v1/auth/login` |
| auth-service | POST | `/api/v1/auth/register` |
| transaction-service | POST | `/api/v1/transactions/deposit` |
| transaction-service | POST | `/api/v1/transactions/withdrawal` |
| auth-service | POST | `/api/v1/auth/register` |
| account-service | POST | `/api/v1/accounts` |
| account-service | GET | `/api/v1/accounts/{accountNumber}` |
| account-service | GET | `/api/v1/accounts/{accountNumber}/balance` |
| account-service | GET | `/api/v1/accounts/{accountNumber}/block` |
| account-service | GET | `/api/v1/accounts/{accountNumber}/deduct` |
| account-service | GET | `/api/v1/accounts/{accountNumber}/credit` |
| transaction-service | POST | `/api/v1/transactions/transfer` |
| transaction-service | GET | `/api/v1/transactions/{transactionId}` |
| transaction-service | GET | `/api/v1/transactions/account/{accountNumber}` |
| transaction-service | POST | `/api/v1/transactions/{transactionId}/verify` |
| payment-service | POST | `/api/v1/payments/create-order` |
| payment-service | POST | `/api/v1/payments/webhook` |
