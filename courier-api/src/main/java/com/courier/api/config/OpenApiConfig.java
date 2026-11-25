package com.courier.api.config;

import com.courier.common.security.ApiKeyAuthFilter;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
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
    public OpenAPI courierApiOpenAPI() {
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
                        .title("Courier API")
                        .description("""
                                Courier is a distributed multi-channel notification
                                delivery platform. This API accepts notification
                                submission requests and returns a tracking ID.
                                
                                Use the Dashboard API (port 8082) to query
                                delivery status and aggregate statistics.
                                """)
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Pranay Joshi")
                                .email("pranay@example.com")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:8080")
                                .description("Local development")));
    }
}