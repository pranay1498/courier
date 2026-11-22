package com.courier.common.security;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/*
 * API keys bound from configuration.
 *
 * Keys live in config rather than a database because this is a single-tenant
 * deployment. A multi-tenant production system would store a hash (Argon2 or
 * bcrypt) per client in the DB, support rotation with overlapping validity
 * windows, and never retain the plaintext key after issuance.
 */
@Data
@ConfigurationProperties(prefix = "courier.security")
public class ApiKeyProperties {

    private List<Client> clients = new ArrayList<>();

    @Data
    public static class Client {

        /** The secret presented in the X-API-Key header. */
        private String key;

        /** Stable identifier for this caller - used as the rate limit bucket. */
        private String clientId;

        /** CLIENT or ADMIN. */
        private String role = "CLIENT";
    }
}
