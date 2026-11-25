package com.courier.api.admin;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/admin")
@RequiredArgsConstructor
@Slf4j
@Tag(
        name = "Admin",
        description = "Administrative operations - DLQ replay and operational tooling"
)
public class ReplayController {

    private final ReplayService replayService;

    @Operation(
            summary = "Replay a DLQ'd notification",
            description = """
                Re-queues a permanently failed notification for redelivery.
                
                Only notifications with status DLQ can be replayed.
                The notification is reset to PENDING and a new Kafka event is
                published with a fresh eventId once the change is committed.
                Status moves to QUEUED when the broker acknowledges it; if the
                publish fails it stays PENDING and the reconciler retries it.
                
                Use this after fixing the underlying delivery issue
                (e.g. correcting a bad webhook URL, restoring an email provider).
                """
    )
    @ApiResponse(responseCode = "200",
            description = "Notification accepted for replay")
    @ApiResponse(responseCode = "404",
            description = "Notification not found")
    @ApiResponse(responseCode = "400",
            description = "Notification is not in DLQ status")
    @PostMapping("/notifications/{id}/replay")
    public ResponseEntity<Map<String, String>> replay(
            @Parameter(description = "Notification UUID to replay")
            @PathVariable UUID id) {

        replayService.replay(id);

        return ResponseEntity.ok(Map.of(
                "message", "Notification accepted for replay",
                "notificationId", id.toString()
        ));
    }
}