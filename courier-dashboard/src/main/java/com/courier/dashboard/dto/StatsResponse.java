package com.courier.dashboard.dto;

import lombok.*;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatsResponse {

    /*
     * Key = channel name (EMAIL, SMS, WEBHOOK).
     * Value = stats for that channel.
     */
    private Map<String, ChannelStats> byChannel;

    /*
     * Aggregate across all channels - sum of all channel stats.
     */
    private ChannelStats overall;

    private Instant generatedAt;
}