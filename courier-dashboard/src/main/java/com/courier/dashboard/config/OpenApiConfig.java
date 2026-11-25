package com.courier.dashboard.config;

import com.courier.common.security.ApiKeyAuthFilter;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    private static final String API_KEY_SCHEME = "apiKey";

    @Bean
    public OpenAPI dashboardOpenAPI() {
        return new OpenAPI()
                /*
                 * Declares the X-API-Key header so Swagger UI shows an
                 * "Authorize" button and attaches the key to every "Try it
                 * out" request. The requirement is global: every endpoint
                 * except the permitAll paths needs it.
                 */
                .components(new Components().addSecuritySchemes(API_KEY_SCHEME,
                        new SecurityScheme()
                                .type(SecurityScheme.Type.APIKEY)
                                .in(SecurityScheme.In.HEADER)
                                .name(ApiKeyAuthFilter.HEADER)
                                .description("API key issued to your client. "
                                        + "In local development use dev-client-key-change-me.")))
                .addSecurityItem(new SecurityRequirement().addList(API_KEY_SCHEME))
                .info(new Info()
                        .title("Courier Dashboard API")
                        .description("""
                                Read-only API for querying notification status
                                and delivery statistics.
                                
                                All status reads are Redis-backed - sub-millisecond
                                response for recently processed notifications.
                                Falls back to PostgreSQL on cache miss.
                                """)
                        .version("1.0.0"))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:8082")
                                .description("Local development")));
    }
}