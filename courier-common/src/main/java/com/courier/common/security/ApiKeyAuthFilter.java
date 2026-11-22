package com.courier.common.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;

/*
 * Authenticates a request from its X-API-Key header.
 *
 * This filter only authenticates. Authorization - deciding whether the
 * authenticated client may reach a given path - is left to the rules in
 * SecurityConfig. Keeping the two separate means a new endpoint is covered
 * by the authorization rules without touching this class.
 */
@Slf4j
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-API-Key";

    private final List<ApiKeyProperties.Client> clients;

    public ApiKeyAuthFilter(ApiKeyProperties properties) {
        this.clients = properties.getClients();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain)
            throws ServletException, IOException {

        String presented = request.getHeader(HEADER);

        if (presented != null && !presented.isBlank()) {
            resolve(presented).ifPresentOrElse(client -> {
                /*
                 * The principal is the clientId, never the key. Everything
                 * downstream - rate limiting, MDC, audit logs - sees a stable
                 * identifier and the secret goes no further than this filter.
                 */
                var auth = new UsernamePasswordAuthenticationToken(
                        client.getClientId(),
                        null,
                        List.of(new SimpleGrantedAuthority("ROLE_" + client.getRole())));
                SecurityContextHolder.getContext().setAuthentication(auth);
                log.debug("Authenticated client={} role={}",
                        client.getClientId(), client.getRole());
            }, () -> log.warn("Rejected unknown API key. path={} ip={}",
                    request.getRequestURI(), request.getRemoteAddr()));
        }

        /*
         * Always continue the chain, even with no credential. If no
         * Authentication was set, the authorization rules reject the request
         * with 401 further down. Short-circuiting here would bypass the
         * permitAll paths (actuator health, Swagger).
         */
        chain.doFilter(request, response);
    }

    /*
     * Constant-time comparison.
     *
     * String.equals() returns on the first differing byte, so response time
     * leaks how many leading characters of a guess were correct - enough to
     * recover a key one character at a time. MessageDigest.isEqual compares
     * the full length regardless of where the first difference falls.
     *
     * The loop over clients is not itself constant-time, which reveals only
     * how many clients are configured. That is not a secret.
     */
    private Optional<ApiKeyProperties.Client> resolve(String presented) {
        byte[] presentedBytes = presented.getBytes(StandardCharsets.UTF_8);
        for (ApiKeyProperties.Client client : clients) {
            if (client.getKey() == null || client.getKey().isBlank()) {
                continue;
            }
            byte[] expected = client.getKey().getBytes(StandardCharsets.UTF_8);
            if (MessageDigest.isEqual(presentedBytes, expected)) {
                return Optional.of(client);
            }
        }
        return Optional.empty();
    }
}
