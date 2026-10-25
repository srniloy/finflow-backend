package com.finflow.authservice.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import io.jsonwebtoken.security.Keys;

@Component
@Slf4j
public class JwtService {

    private final SecretKey key;

    private final long ttlMinutes;

    public JwtService(
            @Value("${auth.jwt.secret}") String secret,
            @Value("${auth.jwt.ttl-minutes}") long ttlMinutes
    ) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        this.ttlMinutes = ttlMinutes;
        log.info("JWT issued with TTL of {} minutes", ttlMinutes);
    }

    /**
     * Generates a JWT for the given user.
     * Validity is 60 minutes - a banking application session should be short-lived.
     */
    public String generateToken(String userId, String name, String email, String accountNumber, String role) {
        Date now = new Date();
        Date expiry = new Date(now.getTime() + ttlMinutes * 60_000);

        return Jwts.builder()
                .subject(userId)
                .claim("uid", userId)
                .claim("name", name)
                .claim("email", email)
                .claim("accountNumber", accountNumber)
                .claim("roles", role)
                .issuedAt(now)
                .expiration(expiry)
                .signWith(key, Jwts.SIG.HS256)
                .compact();
    }

    /** Parses and verifies a token. Throws jjwt exceptions (JwtException) when invalid/expired. */
    public Claims validateToken(String token) {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

}
