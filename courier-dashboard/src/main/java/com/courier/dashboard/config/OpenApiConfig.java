package com.courier.dashboard.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI dashboardOpenAPI() {
        return new OpenAPI()
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