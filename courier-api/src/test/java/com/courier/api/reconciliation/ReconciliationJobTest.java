package com.courier.api.reconciliation;

import com.courier.api.kafka.NotificationProducer;
import com.courier.api.reconciliation.ReconciliationJob;
import com.courier.api.reconciliation.RedisLock;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReconciliationJobTest {

    @Mock
    NotificationRepository repository;
    @Mock
    NotificationProducer producer;
    @Mock
    RedisTemplate<String, String> redisTemplate;
    @Mock
    RedisLock lock;
    @Spy
    MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @InjectMocks
    ReconciliationJob job;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(job, "pendingThreshold", Duration.ofMinutes(5));
        ReflectionTestUtils.setField(job, "driftThreshold", Duration.ofMinutes(10));
    }

    @Test
    void skipsEntirelyWhenAnotherInstanceHoldsLock() {
        when(lock.tryAcquire(eq("reconcile-pending"), any())).thenReturn(Optional.empty());

        job.requeueStuckPending();

        verifyNoInteractions(repository, producer);
    }

    @Test
    void republishesStuckPendingWithFreshEventId() {
        Notification stuck = Notification.builder()
                .id(UUID.randomUUID()).correlationId("c-1")
                .channel(Channel.EMAIL).recipient("a@b.com").content("x")
                .status(NotificationStatus.PENDING).build();

        when(lock.tryAcquire(any(), any())).thenReturn(Optional.of("token"));
        when(repository.findTop100ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(any(), any()))
                .thenReturn(List.of(stuck));
        when(producer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));

        job.requeueStuckPending();

        ArgumentCaptor<NotificationEvent> captor = ArgumentCaptor.forClass(NotificationEvent.class);
        verify(producer).publish(captor.capture());
        assertThat(captor.getValue().getSourceService()).isEqualTo("courier-reconciler");
        verify(repository).transitionStatus(eq(stuck.getId()),
                eq(NotificationStatus.PENDING), eq(NotificationStatus.QUEUED), any());
        verify(lock).release("reconcile-pending", "token");
    }

    @Test
    void releasesLockEvenWhenQueryThrows() {
        when(lock.tryAcquire(any(), any())).thenReturn(Optional.of("token"));
        when(repository.findTop100ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(any(), any()))
                .thenThrow(new RuntimeException("DB down"));

        assertThatThrownBy(() -> job.requeueStuckPending()).isInstanceOf(RuntimeException.class);
        verify(lock).release("reconcile-pending", "token");
    }
}