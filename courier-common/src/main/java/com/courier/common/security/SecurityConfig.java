package com.courier.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

/*
 * Imported explicitly by courier-api and courier-dashboard rather than picked
 * up by component scan. courier-worker exposes no public endpoints and should
 * not start demanding API keys for its own actuator healthcheck.
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(ApiKeyProperties.class)
public class SecurityConfig {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /*
     * Aborts startup in the "prod" profile if keys are placeholders, too short,
     * missing or duplicated; warns in every other profile.
     */
    @Bean
    public ApiKeyStartupValidator apiKeyStartupValidator(ApiKeyProperties properties,
                                                         Environment environment) {
        return new ApiKeyStartupValidator(properties, environment);
    }

    @Bean
    public ApiKeyAuthFilter apiKeyAuthFilter(ApiKeyProperties properties) {
        return new ApiKeyAuthFilter(properties);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http,
                                                   ApiKeyAuthFilter apiKeyAuthFilter)
            throws Exception {
        return http
                /*
                 * CSRF guards against a browser silently attaching an ambient
                 * credential (a cookie) to a cross-site request. This API has
                 * no cookies and no sessions - every request carries its own
                 * header credential - so there is nothing to ride on.
                 */
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s ->
                        s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // Docker healthchecks and load balancers must reach these
                        .requestMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        // Interactive docs stay open so the project is explorable
                        .requestMatchers("/swagger-ui/**", "/swagger-ui.html",
                                "/api-docs/**", "/favicon.ico").permitAll()
                        // Admin operations require the admin key specifically
                        .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
                        .requestMatchers("/api/v1/**").hasAnyRole("CLIENT", "ADMIN")
                        /*
                         * denyAll, not permitAll. Anything not matched above,
                         * including an endpoint added later, is closed by
                         * default. Failing closed is the only safe default -
                         * a forgotten rule becomes a 403, not a data leak.
                         */
                        .anyRequest().denyAll())
                .addFilterBefore(apiKeyAuthFilter,
                        UsernamePasswordAuthenticationFilter.class)
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint(entryPoint())
                        .accessDeniedHandler(accessDeniedHandler()))
                .build();
    }

    /*
     * 401 - no credential presented, or one we do not recognise.
     * The body shape matches GlobalExceptionHandler so clients can parse
     * every error response the same way.
     */
    private AuthenticationEntryPoint entryPoint() {
        return (req, res, ex) -> write(res, HttpStatus.UNAUTHORIZED,
                "UNAUTHORIZED",
                "Missing or invalid " + ApiKeyAuthFilter.HEADER + " header");
    }

    /*
     * 403 - the credential is valid but this client lacks the role.
     * A CLIENT key hitting /api/v1/admin/** lands here, not on 401. The
     * distinction matters: 401 means "who are you", 403 means "I know who
     * you are and the answer is no". Retrying with the same key is futile.
     */
    private AccessDeniedHandler accessDeniedHandler() {
        return (req, res, ex) -> write(res, HttpStatus.FORBIDDEN,
                "FORBIDDEN",
                "This API key is not permitted to access " + req.getRequestURI());
    }

    private void write(HttpServletResponse res, HttpStatus status,
                       String code, String message) throws IOException {
        res.setStatus(status.value());
        res.setContentType(MediaType.APPLICATION_JSON_VALUE);
        MAPPER.writeValue(res.getOutputStream(), Map.of(
                "error", code,
                "message", message,
                "timestamp", Instant.now().toString()));
    }
}
