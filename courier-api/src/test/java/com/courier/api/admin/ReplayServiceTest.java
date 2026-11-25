package com.courier.api.admin;

import com.courier.api.service.EventDispatcher;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.enums.Priority;
import com.courier.common.exception.NotificationNotFoundException;
import com.courier.common.repository.NotificationRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("ReplayService")
class ReplayServiceTest {

    @Mock
    private NotificationRepository notificationRepository;

    @Mock
    private EventDispatcher eventDispatcher;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @InjectMocks
    private ReplayService replayService;

    private Notification notificationWith(NotificationStatus status) {
        return Notification.builder()
                .id(UUID.randomUUID())
                .correlationId("corr-1")
                .channel(Channel.EMAIL)
                .recipient("a@b.com")
                .content("hello")
                .priority(Priority.NORMAL)
                .status(status)
                .errorMessage("SMTP timeout")
                .retryCount(3)
                .build();
    }

    @Test
    @DisplayName("Resets a DLQ notification to PENDING and hands a fresh event to the dispatcher")
    void replaysDlqNotification() {
        Notification dlq = notificationWith(NotificationStatus.DLQ);
        when(notificationRepository.findById(dlq.getId())).thenReturn(Optional.of(dlq));

        replayService.replay(dlq.getId());

        // Reset - and crucially NOT marked QUEUED here; only the broker ack does that
        assertThat(dlq.getStatus()).isEqualTo(NotificationStatus.PENDING);
        assertThat(dlq.getErrorMessage()).isNull();
        assertThat(dlq.getRetryCount()).isZero();

        ArgumentCaptor<NotificationEvent> event = ArgumentCaptor.forClass(NotificationEvent.class);
        verify(eventDispatcher).dispatchAfterCommit(event.capture(), any());
        assertThat(event.getValue().getNotificationId()).isEqualTo(dlq.getId());
        assertThat(event.getValue().getSourceService()).isEqualTo("courier-api-replay");
        assertThat(event.getValue().getEventId()).isNotBlank();

        verify(redisTemplate).delete("status:" + dlq.getId());
    }

    @Test
    @DisplayName("Saves the reset row before dispatching, and saves it exactly once")
    void savesBeforeDispatching() {
        Notification dlq = notificationWith(NotificationStatus.DLQ);
        when(notificationRepository.findById(dlq.getId())).thenReturn(Optional.of(dlq));

        replayService.replay(dlq.getId());

        InOrder order = inOrder(notificationRepository, eventDispatcher);
        order.verify(notificationRepository).save(dlq);
        order.verify(eventDispatcher).dispatchAfterCommit(any(), any());
        // Regression guard: no second save() forcing QUEUED before the ack
        verify(notificationRepository, org.mockito.Mockito.times(1)).save(any());
    }

    @Test
    @DisplayName("Refuses to replay a notification that is not in DLQ (would duplicate a delivery)")
    void rejectsNonDlqNotification() {
        Notification delivered = notificationWith(NotificationStatus.DELIVERED);
        when(notificationRepository.findById(delivered.getId())).thenReturn(Optional.of(delivered));

        assertThatThrownBy(() -> replayService.replay(delivered.getId()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only DLQ");

        verify(notificationRepository, never()).save(any());
        verifyNoInteractions(eventDispatcher);
    }

    @Test
    @DisplayName("Throws not-found for an unknown notification")
    void rejectsUnknownNotification() {
        UUID unknown = UUID.randomUUID();
        when(notificationRepository.findById(unknown)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> replayService.replay(unknown))
                .isInstanceOf(NotificationNotFoundException.class);

        verifyNoInteractions(eventDispatcher);
    }
}
