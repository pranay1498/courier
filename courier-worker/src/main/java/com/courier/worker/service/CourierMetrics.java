package com.courier.worker.service;

import com.courier.common.enums.Channel;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class CourierMetrics {

    private final MeterRegistry registry;

    /*
     * Counter: courier.notifications.total
     * Tags: channel (EMAIL/SMS/WEBHOOK), outcome (delivered/failed/dlq)
     *
     * This lets you query:
     * - Total delivered emails in last hour
     * - Total failed webhooks today
     * - DLQ rate per channel
     *
     * NEVER create a counter inside a hot path loop - always use the
     * registry to look up or create. Micrometer caches counters internally
     * so this is safe to call on every message.
     */
    public void recordDelivered(Channel channel) {
        Counter.builder("courier.notifications.total")
                .tag("channel", channel.name())
                .tag("outcome", "delivered")
                .description("Total notifications processed by outcome and channel")
                .register(registry)
                .increment();
    }

    public void recordFailed(Channel channel) {
        Counter.builder("courier.notifications.total")
                .tag("channel", channel.name())
                .tag("outcome", "failed")
                .description("Total notifications processed by outcome and channel")
                .register(registry)
                .increment();
    }

    public void recordDlq(Channel channel) {
        Counter.builder("courier.notifications.total")
                .tag("channel", channel.name())
                .tag("outcome", "dlq")
                .description("Total notifications processed by outcome and channel")
                .register(registry)
                .increment();
    }

    /*
     * Timer: courier.delivery.duration
     * Tags: channel, outcome
     *
     * Automatically tracks count, total time, max, and percentiles.
     * Percentiles (p50, p95, p99) show latency distribution -
     * averages hide outliers, percentiles reveal them.
     *
     * p50 = median delivery time
     * p95 = 95% of deliveries finish within this time
     * p99 = worst-case latency excluding top 1%
     *
     * publishPercentiles publishes pre-computed percentiles as gauge metrics.
     * publishPercentileHistogram publishes a histogram for Prometheus
     * to compute percentiles server-side (more flexible, more data).
     */
    public void recordDeliveryDuration(Channel channel,
                                       String outcome,
                                       long durationMs) {
        Timer.builder("courier.delivery.duration")
                .tag("channel", channel.name())
                .tag("outcome", outcome)
                .description("Delivery duration in milliseconds")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry)
                .record(Duration.ofMillis(durationMs));
    }
}