package com.courier.api.service;

import com.courier.api.kafka.NotificationProducer;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/*
 * The dispatcher owns the "publish after commit, QUEUED only on broker ack"
 * rules shared by submit and replay, so those rules are tested here once.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("EventDispatcher")
class EventDispatcherTest {

    @Mock
    private NotificationProducer producer;

    @Mock
    private NotificationRepository repository;

    @InjectMocks
    private EventDispatcher dispatcher;

    private final UUID id = UUID.randomUUID();
    private final NotificationEvent event = NotificationEvent.builder()
            .eventId("event-1")
            .notificationId(id)
            .channel(Channel.EMAIL)
            .build();

    @AfterEach
    void clearTransactionState() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    // ── No transaction: publish immediately ────────────────────────────────

    @Test
    @DisplayName("Marks the row QUEUED (PENDING -> QUEUED compare-and-set) when the broker acks")
    void transitionsToQueuedOnAck() {
        when(producer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatchAfterCommit(event, id);

        verify(repository).transitionStatus(
                eq(id), eq(NotificationStatus.PENDING), eq(NotificationStatus.QUEUED), any());
    }

    @Test
    @DisplayName("Does not touch the row until the ack actually arrives")
    void waitsForTheAck() {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, NotificationEvent>> ack =
                new CompletableFuture<>();
        when(producer.publish(any())).thenReturn(ack);

        dispatcher.dispatchAfterCommit(event, id);
        verify(repository, never()).transitionStatus(any(), any(), any(), any());

        ack.complete(null);
        verify(repository).transitionStatus(
                eq(id), eq(NotificationStatus.PENDING), eq(NotificationStatus.QUEUED), any());
    }

    @Test
    @DisplayName("Leaves the row PENDING when the broker does not ack, so the reconciler retries it")
    void leavesRowPendingWhenPublishFails() {
        when(producer.publish(any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        dispatcher.dispatchAfterCommit(event, id);

        verify(repository, never()).transitionStatus(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Swallows a synchronous publish failure - the row is committed and the reconciler will retry")
    void swallowsSynchronousPublishFailure() {
        when(producer.publish(any())).thenThrow(new IllegalStateException("producer closed"));

        dispatcher.dispatchAfterCommit(event, id);   // must not throw

        verify(repository, never()).transitionStatus(any(), any(), any(), any());
    }

    // ── Inside a transaction: publish only after commit ────────────────────

    @Test
    @DisplayName("Inside a transaction, nothing is published until the commit happens")
    void publishesOnlyAfterCommit() {
        TransactionSynchronizationManager.initSynchronization();
        when(producer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatchAfterCommit(event, id);

        // Transaction still open: the row is not visible to anyone else yet
        verifyNoInteractions(producer);

        TransactionSynchronizationManager.getSynchronizations()
                .forEach(TransactionSynchronization::afterCommit);

        verify(producer).publish(event);
        verify(repository).transitionStatus(
                eq(id), eq(NotificationStatus.PENDING), eq(NotificationStatus.QUEUED), any());
    }

    @Test
    @DisplayName("A rolled-back transaction publishes nothing")
    void rollbackPublishesNothing() {
        TransactionSynchronizationManager.initSynchronization();

        dispatcher.dispatchAfterCommit(event, id);

        TransactionSynchronizationManager.getSynchronizations()
                .forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));

        verifyNoInteractions(producer);
        verifyNoInteractions(repository);
    }
}
