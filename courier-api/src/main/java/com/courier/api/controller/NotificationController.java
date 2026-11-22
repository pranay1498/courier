package com.courier.api.controller;

import com.courier.api.service.NotificationService;
import com.courier.common.dto.NotificationRequest;
import com.courier.common.dto.NotificationResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/notifications")
@RequiredArgsConstructor
@Slf4j
@Tag(
        name = "Notifications",
        description = "Submit notifications for delivery across Email, SMS, and Webhook channels"
)
public class NotificationController {

    private final NotificationService notificationService;

    /*
     * POST /api/v1/notifications
     *
     * @Valid triggers Bean Validation on the request body.
     * If any @NotBlank / @NotNull / @Size constraint fails,
     * Spring throws MethodArgumentNotValidException BEFORE
     * this method body runs. The GlobalExceptionHandler
     * converts it to HTTP 400 with a clear error message.
     *
     * Returns HTTP 202 Accepted - not 200 OK.
     * 200 means "I did it."
     * 202 means "I've accepted your request and will process it."
     * The client should poll /status/{id} for the final result.
     */
    @Operation(
            summary = "Submit a notification",
            description = """
            Accepts a notification request and queues it for async delivery.
            
            Returns HTTP 202 Accepted immediately - delivery happens asynchronously.
            Use the notificationId in the response to poll /status/{id} for updates,
            or query the Dashboard API for real-time Redis-backed status.
            
            Idempotent: submitting the same correlationId twice returns the
            same notificationId without re-queuing the notification.
            """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202",
                    description = "Notification accepted and queued for delivery"),
            @ApiResponse(responseCode = "400",
                    description = "Invalid request - missing required fields or validation failure"),
            @ApiResponse(responseCode = "429",
                    description = "Rate limit exceeded - see RateLimiterService for the configured limit"),
            @ApiResponse(responseCode = "500",
                    description = "Internal error - notification may not have been queued")
    })
    @PostMapping
    public ResponseEntity<NotificationResponse> submit(
            @Valid @RequestBody NotificationRequest request,
            Principal principal) {

        /*
         * Principal is resolved by Spring from the SecurityContext that
         * ApiKeyAuthFilter populated. getName() is the clientId. The service
         * receives a plain String and stays free of any Spring Security
         * coupling, which keeps it unit-testable with no security context.
         */
        String clientId = principal.getName();

        /*
         * MDC (Mapped Diagnostic Context) adds correlationId to
         * every log line produced during this request's lifecycle.
         * When you grep logs for a correlationId, you see the
         * complete trace of that single request - validation,
         * DB save, Kafka publish, all in one view.
         *
         * MDC is thread-local - it's isolated per request thread.
         * The finally block clears it after the request completes
         * so it doesn't leak into the next request on the same thread.
         */
        MDC.put("correlationId", request.getCorrelationId());
        MDC.put("channel", request.getChannel().name());
        MDC.put("clientId", clientId);

        try {
            NotificationResponse response = notificationService.submit(request, clientId);
            return ResponseEntity.accepted().body(response); // HTTP 202
        } finally {
            MDC.clear();
        }
    }

    /*
     * GET /api/v1/notifications/{id}/status
     *
     * Allows the client to poll for the current status of a notification.
     * Returns 200 with the current state, or 404 if not found.
     */
    @Operation(
            summary = "Get notification status",
            description = """
            Returns the current status of a notification by its ID.
            
            Reads directly from PostgreSQL. For faster Redis-backed status reads,
            use GET /api/v1/dashboard/notifications/{id} on port 8082.
            """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Notification found"),
            @ApiResponse(responseCode = "404", description = "Notification not found")
    })
    @GetMapping("/{id}/status")
    public ResponseEntity<NotificationResponse> getStatus(
            @Parameter(description = "Notification UUID returned at submission")
            @PathVariable UUID id) {

        MDC.put("notificationId", id.toString());
        try {
            return ResponseEntity.ok(notificationService.getStatus(id));
        } finally {
            MDC.clear();
        }
    }
}