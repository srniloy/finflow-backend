package com.finflow.apigateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Global JWT validation filter.
 *
 * - Public endpoints (register/login, actuator) are allowed without a token.
 * - Every other request must carry "Authorization: Bearer <token>" signed with
 *   the shared HS256 secret. Because tokens expire after 60 minutes (issued
 *   by the auth-service), this doubles as the client session timeout.
 */
@Component
@Slf4j
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

    /** Paths that never require a Bearer token. */
    private static final List<String> PUBLIC_PATHS = List.of(
            "/api/v1/auth/register",
            "/api/v1/auth/login",
            "/actuator"
    );

    private final SecretKey key;

    public JwtAuthenticationFilter(@Value("${api.jwt.secret}") String secret) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        log.info("Gateway JWT validation enabled (HS256 shared secret)");
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();

        if (isPublic(path)) {
            return chain.filter(exchange);
        }

        String auth = exchange.getRequest().getHeaders().getFirst("Authorization");
        if (auth == null || !auth.startsWith("Bearer ")) {
            return respondUnauthorized(exchange);
        }

        try {
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(auth.substring("Bearer ".length()))
                    .getPayload();

            // Token signature + expiry already verified by the parser.
            log.info("Authenticated request to {} for user {}", path, claims.get("uid"));

            // Forward identity headers to the downstream service.
            return chain.filter(exchange.mutate()
                    .request(exchange.getRequest().mutate()
                            .header("X-User-Id", String.valueOf(claims.get("uid")))
                            .header("X-Account-Number", String.valueOf(claims.get("accountNumber")))
                            .build())
                    .build());
        }
        catch (JwtException e) {
            log.warn("Rejected request to {}: {}", path, e.getMessage());
            return respondUnauthorized(exchange);
        }
    }

    private boolean isPublic(String path) {
        return PUBLIC_PATHS.stream().anyMatch(p -> path.startsWith(p));
    }

    private Mono<Void> respondUnauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().add("Content-Type", "application/json");
        String body = "{" +
                "\"timestamp\": \"" + LocalDateTime.now() + "\"," +
                "\"status\": 401," +
                "\"error\": \"Unauthorized\"," +
                "\"message\": \"Missing or invalid authentication token (session expired after 60 minutes?)\"" +
                "}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        return exchange.getResponse()
                .writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

}
