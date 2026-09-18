package com.courier.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI courierApiOpenAPI() {
        return new OpenAPI()
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