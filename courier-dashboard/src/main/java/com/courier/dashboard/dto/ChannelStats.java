package com.courier.dashboard.dto;

import lombok.*;

/*
 * Delivery statistics for one channel.
 * All counts come from Redis Hash counters -
 * incremented by courier-worker on every delivery outcome.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChannelStats {

    private long total;
    private long delivered;
    private long failed;
    private long dlq;
    private long pending;

    /*
     * Derived - not stored separately.
     * Computed here rather than in the controller
     * to keep the controller free of logic.
     */
    public double getDeliveryRate() {
        if (total == 0) return 0.0;
        return Math.round((delivered * 100.0 / total) * 100.0) / 100.0;
    }

    /*
     * Build from the Redis Hash entries map.
     * Redis stores all values as Strings -
     * parse defensively, default to 0 on missing or malformed values.
     */
    public static ChannelStats from(java.util.Map<Object, Object> entries) {
        return ChannelStats.builder()
                .total(parseLong(entries.get("total")))
                .delivered(parseLong(entries.get("delivered")))
                .failed(parseLong(entries.get("failed")))
                .dlq(parseLong(entries.get("dlq")))
                .pending(parseLong(entries.get("pending")))
                .build();
    }

    private static long parseLong(Object value) {
        if (value == null) return 0L;
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}