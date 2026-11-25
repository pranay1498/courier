package com.courier.api.security;

import com.courier.common.security.ApiKeyProperties;
import com.courier.common.security.ApiKeyStartupValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ApiKeyStartupValidator")
class ApiKeyStartupValidatorTest {

    private static final String STRONG_A = "a-very-long-random-secret-1234567890";
    private static final String STRONG_B = "another-very-long-random-secret-0987";

    private static ApiKeyProperties.Client client(String id, String key, String role) {
        ApiKeyProperties.Client c = new ApiKeyProperties.Client();
        c.setClientId(id);
        c.setKey(key);
        c.setRole(role);
        return c;
    }

    private static ApiKeyStartupValidator validator(String profile, ApiKeyProperties.Client... clients) {
        ApiKeyProperties props = new ApiKeyProperties();
        props.setClients(List.of(clients));
        MockEnvironment env = new MockEnvironment();
        if (profile != null) {
            env.setActiveProfiles(profile);
        }
        return new ApiKeyStartupValidator(props, env);
    }

    // ── prod: unsafe configuration aborts startup ──────────────────────────

    @Test
    @DisplayName("prod: refuses to start with the placeholder default key")
    void prodRejectsPlaceholderKey() {
        var v = validator("prod",
                client("dev-client", "dev-client-key-change-me", "CLIENT"),
                client("prod-admin", STRONG_A, "ADMIN"));

        assertThatThrownBy(v::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dev-client")
                .hasMessageContaining("placeholder");
    }

    @Test
    @DisplayName("prod: the error names the client but never leaks the key value")
    void errorMessageNeverContainsTheKey() {
        var v = validator("prod", client("dev-client", "dev-client-key-change-me", "CLIENT"));

        assertThatThrownBy(v::afterPropertiesSet)
                .hasMessageNotContaining("dev-client-key-change-me");
    }

    @Test
    @DisplayName("prod: refuses a key that is too short")
    void prodRejectsShortKey() {
        var v = validator("prod", client("c1", "short-key", "CLIENT"));

        assertThatThrownBy(v::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shorter than");
    }

    @Test
    @DisplayName("prod: refuses two clients sharing one key")
    void prodRejectsDuplicateKeys() {
        var v = validator("prod",
                client("c1", STRONG_A, "CLIENT"),
                client("c2", STRONG_A, "ADMIN"));

        assertThatThrownBy(v::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shares its key");
    }

    @Test
    @DisplayName("prod: refuses a blank key")
    void prodRejectsBlankKey() {
        var v = validator("prod", client("c1", "  ", "CLIENT"));

        assertThatThrownBy(v::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no key");
    }

    @Test
    @DisplayName("prod: refuses a mistyped role, which would otherwise silently grant nothing")
    void prodRejectsInvalidRole() {
        var v = validator("prod", client("c1", STRONG_A, "ADMN"));

        assertThatThrownBy(v::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid role");
    }

    @Test
    @DisplayName("prod: refuses to start with no clients configured")
    void prodRejectsNoClients() {
        var v = validator("prod");

        assertThatThrownBy(v::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no API clients");
    }

    @Test
    @DisplayName("The 'production' profile name is treated the same as 'prod'")
    void productionProfileAlsoEnforces() {
        var v = validator("production", client("dev-client", "dev-client-key-change-me", "CLIENT"));

        assertThatThrownBy(v::afterPropertiesSet).isInstanceOf(IllegalStateException.class);
    }

    // ── prod: a safe configuration starts normally ─────────────────────────

    @Test
    @DisplayName("prod: strong, unique keys start normally")
    void prodAcceptsStrongUniqueKeys() {
        var v = validator("prod",
                client("billing-service", STRONG_A, "CLIENT"),
                client("ops-admin", STRONG_B, "ADMIN"));

        assertThatCode(v::afterPropertiesSet).doesNotThrowAnyException();
    }

    // ── everything else: warn, never block ─────────────────────────────────

    @Test
    @DisplayName("default profile: placeholder keys only warn, so local dev and tests still work")
    void defaultProfileDoesNotBlock() {
        var v = validator(null,
                client("dev-client", "dev-client-key-change-me", "CLIENT"),
                client("dev-admin", "dev-admin-key-change-me", "ADMIN"));

        assertThatCode(v::afterPropertiesSet).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("dev profile: placeholder keys only warn")
    void devProfileDoesNotBlock() {
        var v = validator("dev", client("dev-client", "dev-client-key-change-me", "CLIENT"));

        assertThatCode(v::afterPropertiesSet).doesNotThrowAnyException();
    }
}
