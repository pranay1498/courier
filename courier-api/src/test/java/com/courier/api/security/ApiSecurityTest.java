package com.courier.api.security;

import com.courier.api.admin.ReplayController;
import com.courier.api.admin.ReplayService;
import com.courier.api.controller.NotificationController;
import com.courier.api.service.NotificationService;
import com.courier.common.dto.NotificationResponse;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.security.SecurityConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/*
 * Exercises the REAL security filter chain (unlike GlobalExceptionHandlerTest,
 * which switches filters off) against the real controllers, with the services
 * mocked. Only the web layer is loaded - no Kafka, Redis or DB.
 *
 * Test keys are set explicitly so the assertions don't depend on whatever
 * defaults application.yml ships with. A list bound from a higher-priority
 * property source replaces the yml list rather than merging with it.
 */
@WebMvcTest({NotificationController.class, ReplayController.class})
@Import(SecurityConfig.class)
@TestPropertySource(properties = {
        "courier.security.clients[0].key=client-secret",
        "courier.security.clients[0].client-id=client-a",
        "courier.security.clients[0].role=CLIENT",
        "courier.security.clients[1].key=admin-secret",
        "courier.security.clients[1].client-id=admin-a",
        "courier.security.clients[1].role=ADMIN"
})
@DisplayName("API security (filter chain + authorization rules)")
class ApiSecurityTest {

    private static final String HEADER = "X-API-Key";
    private static final String SUBMIT = "/api/v1/notifications";
    private static final String BODY = """
            {
              "correlationId": "corr-1",
              "channel": "EMAIL",
              "recipient": "a@b.com",
              "content": "hello"
            }
            """;

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NotificationService notificationService;

    @MockBean
    private ReplayService replayService;

    @BeforeEach
    void stubService() {
        NotificationResponse accepted = NotificationResponse.builder()
                .notificationId(UUID.randomUUID())
                .correlationId("corr-1")
                .channel(Channel.EMAIL)
                .status(NotificationStatus.PENDING)
                .build();
        when(notificationService.submit(any(), any())).thenReturn(accepted);
        when(notificationService.getStatus(any())).thenReturn(accepted);
    }

    // ── 401: not authenticated ─────────────────────────────────────────────

    @Test
    @DisplayName("401 with a JSON error body when no API key is sent")
    void missingKeyIsUnauthorized() throws Exception {
        mockMvc.perform(post(SUBMIT).contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error").value("UNAUTHORIZED"))
                .andExpect(jsonPath("$.message").exists())
                .andExpect(jsonPath("$.timestamp").exists());

        verify(notificationService, never()).submit(any(), any());
    }

    @Test
    @DisplayName("401 when the API key is unknown")
    void unknownKeyIsUnauthorized() throws Exception {
        mockMvc.perform(post(SUBMIT).header(HEADER, "not-a-real-key")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isUnauthorized());

        verify(notificationService, never()).submit(any(), any());
    }

    @Test
    @DisplayName("401 when the API key header is blank")
    void blankKeyIsUnauthorized() throws Exception {
        mockMvc.perform(post(SUBMIT).header(HEADER, "   ")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("A key that merely starts with a valid key is rejected (no prefix matching)")
    void keyPrefixIsNotAccepted() throws Exception {
        mockMvc.perform(post(SUBMIT).header(HEADER, "client-secret-extra")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isUnauthorized());
    }

    // ── 202 / 200: authenticated and authorized ────────────────────────────

    @Test
    @DisplayName("Client key can submit, and the service receives the clientId - never the key")
    void clientKeyCanSubmit() throws Exception {
        mockMvc.perform(post(SUBMIT).header(HEADER, "client-secret")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"));

        // This is what the rate limiter keys on: a stable per-client id.
        verify(notificationService).submit(any(), eq("client-a"));
    }

    @Test
    @DisplayName("Client key can read notification status")
    void clientKeyCanReadStatus() throws Exception {
        mockMvc.perform(get(SUBMIT + "/" + UUID.randomUUID() + "/status")
                        .header(HEADER, "client-secret"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("Admin key can also use the normal client endpoints")
    void adminKeyCanSubmit() throws Exception {
        mockMvc.perform(post(SUBMIT).header(HEADER, "admin-secret")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isAccepted());

        verify(notificationService).submit(any(), eq("admin-a"));
    }

    // ── 403: authenticated but not authorized ──────────────────────────────

    @Test
    @DisplayName("403 (not 401) when a client key calls an admin endpoint")
    void clientKeyCannotCallAdmin() throws Exception {
        UUID id = UUID.randomUUID();

        mockMvc.perform(post("/api/v1/admin/notifications/" + id + "/replay")
                        .header(HEADER, "client-secret"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("FORBIDDEN"));

        verify(replayService, never()).replay(any());
    }

    @Test
    @DisplayName("Admin key can call admin endpoints")
    void adminKeyCanCallAdmin() throws Exception {
        UUID id = UUID.randomUUID();

        mockMvc.perform(post("/api/v1/admin/notifications/" + id + "/replay")
                        .header(HEADER, "admin-secret"))
                .andExpect(status().isOk());

        verify(replayService).replay(id);
    }

    @Test
    @DisplayName("No key on an admin endpoint is 401 - the caller is unknown, not forbidden")
    void anonymousOnAdminIsUnauthorized() throws Exception {
        mockMvc.perform(post("/api/v1/admin/notifications/" + UUID.randomUUID() + "/replay"))
                .andExpect(status().isUnauthorized());
    }

    // ── Fail closed / open paths ───────────────────────────────────────────

    @Test
    @DisplayName("Unlisted paths are denied by default, even with a valid admin key")
    void unlistedPathIsDeniedByDefault() throws Exception {
        mockMvc.perform(get("/internal/anything").header(HEADER, "admin-secret"))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("Health endpoint is reachable without a key (healthchecks, load balancers)")
    void healthIsPublic() throws Exception {
        // No actuator is loaded in this slice, so nothing serves this path.
        // Security must let the request through (not 401/403), and the
        // exception handler must report it as a 404 rather than a 500.
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("NOT_FOUND"));
    }
}
