package com.courier.dashboard.controller;

import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.exception.NotificationNotFoundException;
import com.courier.dashboard.dto.*;
import com.courier.dashboard.service.DashboardService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dashboard")
@RequiredArgsConstructor
@Slf4j
@Tag(
        name = "Dashboard",
        description = "Redis-backed read API for status queries and delivery statistics"
)
public class DashboardController {

    private final DashboardService dashboardService;

    /*
     * GET /api/v1/dashboard/notifications/{id}
     *
     * Primary status endpoint - reads from Redis first.
     * This is what clients should use instead of the courier-api
     * status endpoint, which reads directly from DB.
     */
    @Operation(
            summary = "Get notification status (Redis-backed)",
            description = """
                Returns current notification status.
                Reads from Redis first - sub-millisecond for recent notifications.
                Falls back to PostgreSQL on cache miss and repopulates the cache.
                Response includes a 'source' field indicating REDIS or DB.
                """
    )
    @ApiResponse(responseCode = "200", description = "Status returned")
    @ApiResponse(responseCode = "404", description = "Notification not found")
    @GetMapping("/notifications/{id}")
    public ResponseEntity<NotificationStatusResponse> getStatus(
            @Parameter(description = "Notification UUID")
            @PathVariable UUID id) {
        try {
            return ResponseEntity.ok(dashboardService.getStatus(id));
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    /*
     * GET /api/v1/dashboard/stats
     *
     * Aggregate delivery statistics per channel.
     * Reads from Redis counters - sub-millisecond response.
     * Falls back to DB if Redis counters are empty.
     */
    @Operation(
            summary = "Get delivery statistics",
            description = """
                Returns aggregate delivery stats per channel (EMAIL, SMS, WEBHOOK).
                Reads from Redis Hash counters - O(1) per channel.
                Falls back to PostgreSQL COUNT queries if Redis counters are empty.
                """
    )
    @GetMapping("/stats")
    public ResponseEntity<StatsResponse> getStats() {
        return ResponseEntity.ok(dashboardService.getStats());
    }

    /*
     * GET /api/v1/dashboard/notifications
     *
     * Paginated listing with optional filters.
     * ?status=FAILED&channel=EMAIL&page=0&size=20&sort=createdAt,desc
     *
     * @PageableDefault sets defaults when no pagination params provided.
     * Without it, ?page=0&size=20 is the Spring default.
     * With it, you control the default page size and sort.
     */
    @Operation(
            summary = "List notifications",
            description = """
                Paginated listing of notifications with optional filters.
                Supports: ?status=DELIVERED&channel=EMAIL&page=0&size=20&sort=createdAt,desc
                Always reads from PostgreSQL - listing is not cached.
                """
    )
    @GetMapping("/notifications")
    public ResponseEntity<Page<NotificationSummary>> list(
            @Parameter(description = "Filter by status")
            @RequestParam(required = false) NotificationStatus status,
            @Parameter(description = "Filter by channel")
            @RequestParam(required = false) Channel channel,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        return ResponseEntity.ok(
                dashboardService.list(status, channel, pageable));
    }

    @Operation(
            summary = "Get delivery attempts",
            description = """
            Returns all delivery attempts for a notification in order.
            Shows the full retry history - each attempt's outcome,
            error message if failed, and timestamp.
            """
    )
    @ApiResponse(responseCode = "200", description = "Attempts returned")
    @ApiResponse(responseCode = "404", description = "Notification not found")
    @GetMapping("/notifications/{id}/attempts")
    public ResponseEntity<List<DeliveryAttemptResponse>> getAttempts(
            @Parameter(description = "Notification UUID")
            @PathVariable UUID id) {
            return ResponseEntity.ok(dashboardService.getDeliveryAttempts(id));
    }


}