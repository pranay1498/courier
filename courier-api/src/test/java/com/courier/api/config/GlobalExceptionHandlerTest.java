package com.courier.api.config;

import com.courier.api.controller.NotificationController;
import com.courier.api.service.NotificationService;
import com.courier.common.exception.NotificationNotFoundException;
import com.courier.common.exception.RateLimitExceededException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/*
 * @WebMvcTest loads only the web layer - controller, filters,
 * exception handlers. No Kafka, Redis, DB beans loaded.
 * Much faster than @SpringBootTest for testing HTTP behaviour.
 */
@WebMvcTest(NotificationController.class)
@DisplayName("GlobalExceptionHandler Tests")
class GlobalExceptionHandlerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NotificationService notificationService;

    @Test
    @DisplayName("Should return 400 when required fields are missing")
    void shouldReturn400ForMissingRequiredFields() throws Exception {
        mockMvc.perform(post("/api/v1/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "channel": "EMAIL",
                                "recipient": "test@example.com",
                                "content": "Missing correlationId"
                            }
                            """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error")
                        .value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    @DisplayName("Should return 429 with Retry-After header when rate limited")
    void shouldReturn429WithRetryAfterHeader() throws Exception {
        when(notificationService.submit(any()))
                .thenThrow(new RateLimitExceededException(
                        "Rate limit exceeded for channel: EMAIL"));

        mockMvc.perform(post("/api/v1/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "correlationId": "test-001",
                                "channel": "EMAIL",
                                "recipient": "test@example.com",
                                "content": "Test"
                            }
                            """))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("Retry-After", "60"))
                .andExpect(jsonPath("$.error")
                        .value("RATE_LIMIT_EXCEEDED"));
    }

    @Test
    @DisplayName("Should return 404 when notification not found")
    void shouldReturn404WhenNotificationNotFound() throws Exception {
        UUID id = UUID.randomUUID();
        when(notificationService.getStatus(id))
                .thenThrow(new NotificationNotFoundException(id));

        mockMvc.perform(get("/api/v1/notifications/{id}/status", id))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Should return 500 with generic message for unhandled exceptions")
    void shouldReturn500WithGenericMessageForUnhandledException() throws Exception {
        when(notificationService.submit(any()))
                .thenThrow(new RuntimeException("DB connection pool exhausted"));

        mockMvc.perform(post("/api/v1/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "correlationId": "test-001",
                                "channel": "EMAIL",
                                "recipient": "test@example.com",
                                "content": "Test"
                            }
                            """))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error")
                        .value("INTERNAL_ERROR"))
                /*
                 * Critical: the actual exception message must NOT appear
                 * in the response - it could expose internal system details.
                 */
                .andExpect(jsonPath("$.message")
                        .value("An unexpected error occurred. Please try again later."));
    }

    @Test
    @DisplayName("Should return 400 with field-level errors for validation failure")
    void shouldReturn400WithFieldErrors() throws Exception {
        mockMvc.perform(post("/api/v1/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "correlationId": "",
                                "channel": "EMAIL",
                                "recipient": "test@example.com",
                                "content": "Test"
                            }
                            """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error")
                        .value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.message")
                        .value(org.hamcrest.Matchers
                                .containsString("correlationId")));
    }
}