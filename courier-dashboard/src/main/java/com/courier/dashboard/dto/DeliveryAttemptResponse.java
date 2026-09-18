package com.courier.dashboard.dto;

import com.courier.common.entity.DeliveryAttempt;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryAttemptResponse {

    private UUID id;
    private int attemptNumber;
    private String status;
    private String errorMessage;
    private Instant attemptedAt;

    public static DeliveryAttemptResponse from(DeliveryAttempt attempt) {
        return DeliveryAttemptResponse.builder()
                .id(attempt.getId())
                .attemptNumber(attempt.getAttemptNumber())
                .status(attempt.getStatus())
                .errorMessage(attempt.getErrorMessage())
                .attemptedAt(attempt.getAttemptedAt())
                .build();
    }
}