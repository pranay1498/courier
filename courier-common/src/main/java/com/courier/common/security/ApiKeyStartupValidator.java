package com.courier.common.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * Refuses to start in production with an unsafe API key configuration.
 *
 * The defaults in application.yml (dev-*-key-change-me) exist so that
 * docker-compose and the README curl work with no setup. They are public in
 * the repository, so a deployment that forgets to set COURIER_CLIENT_KEY /
 * COURIER_ADMIN_KEY would otherwise start happily with an admin key anyone
 * can read on GitHub. The name "change-me" is only a convention; this class
 * is the safeguard.
 *
 * With the "prod" profile active any problem aborts startup. In every other
 * profile the same problems are logged as warnings, so local development and
 * tests still work out of the box.
 *
 * Messages name the client (or its position), never the key itself - a
 * secret must not end up in logs.
 */
@Slf4j
public class ApiKeyStartupValidator implements InitializingBean {

    static final int MIN_KEY_LENGTH = 24;
    private static final String PLACEHOLDER_MARKER = "change-me";
    private static final Set<String> VALID_ROLES = Set.of("CLIENT", "ADMIN");

    private final ApiKeyProperties properties;
    private final boolean production;

    public ApiKeyStartupValidator(ApiKeyProperties properties, Environment environment) {
        this.properties = properties;
        this.production = environment.acceptsProfiles(Profiles.of("prod", "production"));
    }

    @Override
    public void afterPropertiesSet() {
        List<String> problems = findProblems(properties);
        if (problems.isEmpty()) {
            return;
        }
        String summary = String.join("; ", problems);
        if (production) {
            throw new IllegalStateException(
                    "Refusing to start: unsafe API key configuration: " + summary
                            + ". Set COURIER_CLIENT_KEY and COURIER_ADMIN_KEY to strong, "
                            + "unique secrets.");
        }
        log.warn("API key configuration is NOT production-safe (ignored outside the " +
                "'prod' profile): {}", summary);
    }

    static List<String> findProblems(ApiKeyProperties props) {
        List<String> problems = new ArrayList<>();
        List<ApiKeyProperties.Client> clients = props.getClients();

        if (clients == null || clients.isEmpty()) {
            problems.add("no API clients are configured");
            return problems;
        }

        Set<String> seenKeys = new HashSet<>();
        Set<String> seenClientIds = new HashSet<>();

        for (int i = 0; i < clients.size(); i++) {
            ApiKeyProperties.Client c = clients.get(i);
            String who = (c.getClientId() == null || c.getClientId().isBlank())
                    ? "client #" + i
                    : "client '" + c.getClientId() + "'";

            if (c.getClientId() == null || c.getClientId().isBlank()) {
                problems.add(who + " has no clientId");
            } else if (!seenClientIds.add(c.getClientId())) {
                problems.add(who + " is defined more than once");
            }

            if (c.getRole() == null || !VALID_ROLES.contains(c.getRole())) {
                problems.add(who + " has an invalid role (expected CLIENT or ADMIN)");
            }

            String key = c.getKey();
            if (key == null || key.isBlank()) {
                problems.add(who + " has no key");
                continue;
            }
            if (key.toLowerCase().contains(PLACEHOLDER_MARKER)) {
                problems.add(who + " is using the placeholder default key");
            }
            if (key.length() < MIN_KEY_LENGTH) {
                problems.add(who + " has a key shorter than " + MIN_KEY_LENGTH + " characters");
            }
            if (!seenKeys.add(key)) {
                problems.add(who + " shares its key with another client");
            }
        }
        return problems;
    }
}
