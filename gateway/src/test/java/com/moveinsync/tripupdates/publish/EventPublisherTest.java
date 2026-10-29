package com.moveinsync.tripupdates.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import com.moveinsync.tripupdates.notify.EventNotification;
import com.moveinsync.tripupdates.notify.NotificationPublishException;
import com.moveinsync.tripupdates.notify.NotificationPublisher;
import com.moveinsync.tripupdates.routing.EventRouter;
import com.moveinsync.tripupdates.routing.RoutingException;
import com.moveinsync.tripupdates.store.AppendResult;
import com.moveinsync.tripupdates.store.EventStore;
import com.moveinsync.tripupdates.store.EventStoreUnavailableException;
import com.moveinsync.tripupdates.store.TopicHistory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class EventPublisherTest {

    private static final List<TopicId> TOPICS = List.of(TopicId.trip("T-1"), TopicId.cab("C-1"));

    private final CountingStore store = new CountingStore();
    private final FlakyPublisher notifications = new FlakyPublisher();
    private final EventPublisher publisher = new EventPublisher(store, notifications);

    private static TripEvent status(String eventId) {
        return new TripEvent(eventId, "acme", "T-1", "C-1", Instant.parse("2026-10-29T10:00:00Z"),
                new StatusPayload(TripStatus.ARRIVED));
    }

    @Test
    void notifiesWithTheSequencesOfTheDurableAppend() {
        PublishOutcome outcome = publisher.publish(status("e1"), TOPICS);

        assertThat(outcome.notified()).isTrue();
        assertThat(notifications.sent).containsExactly(EventNotification.of(outcome.appended()));
        assertThat(notifications.sent.get(0).topics()).extracting(EventNotification.TopicSeq::seq)
                .containsExactlyElementsOf(outcome.appended().events().stream().map(SequencedEvent::seq).toList());
    }

    @Test
    void routedPublishAppendsToTheDirectoryDestinationsInOneAppend() {
        EventPublisher routed = new EventPublisher(store, notifications,
                new EventRouter(DevFixtures.load("dev/dev-fixtures.json").directory()));

        PublishOutcome outcome = routed.publish(status("e1"));

        assertThat(store.appends).isEqualTo(1);
        assertThat(outcome.appended().events()).extracting(SequencedEvent::topic)
                .containsExactly(TopicId.trip("T-1"), TopicId.cab("C-1"), TopicId.office("O-1"));
        assertThat(notifications.sent).containsExactly(EventNotification.of(outcome.appended()));
    }

    @Test
    void unroutableEventIsRejectedBeforeAnythingIsWrittenOrNotified() {
        EventPublisher routed = new EventPublisher(store, notifications,
                new EventRouter(DevFixtures.load("dev/dev-fixtures.json").directory()));
        TripEvent wrongCab = new TripEvent("e1", "acme", "T-1", "C-2", Instant.parse("2026-10-29T10:00:00Z"),
                new StatusPayload(TripStatus.ARRIVED));

        assertThatThrownBy(() -> routed.publish(wrongCab)).isInstanceOf(RoutingException.class);
        assertThat(store.appends).isZero();
        assertThat(notifications.attempts).isZero();
    }

    @Test
    void routedPublishRequiresARouter() {
        assertThatThrownBy(() -> publisher.publish(status("e1"))).isInstanceOf(IllegalStateException.class);
        assertThat(store.appends).isZero();
    }

    @Test
    void publishFailureAfterDurableAppendIsReportedAndNeverAppendsAgain() {
        notifications.failing = true;

        PublishOutcome outcome = publisher.publish(status("e1"), TOPICS);

        assertThat(outcome.notified()).isFalse();
        assertThat(store.appends).isEqualTo(1);
        assertThat(store.events).hasSize(1);
        assertThat(publisher.notificationFailures()).isEqualTo(1);

        notifications.failing = false;
        assertThat(publisher.renotify(outcome.appended())).isTrue();
        assertThat(store.appends).as("renotify never appends").isEqualTo(1);
        assertThat(notifications.sent).containsExactly(EventNotification.of(outcome.appended()));
    }

    @Test
    void failedAppendSendsNoNotification() {
        store.failing = true;
        assertThatThrownBy(() -> publisher.publish(status("e1"), TOPICS))
                .isInstanceOf(EventStoreUnavailableException.class);
        assertThat(notifications.sent).isEmpty();
        assertThat(notifications.attempts).isZero();
    }

    @Test
    void idempotentRetryReNotifiesTheOriginalSequences() {
        notifications.failing = true;
        PublishOutcome lost = publisher.publish(status("e1"), TOPICS);
        notifications.failing = false;

        PublishOutcome retry = publisher.publish(status("e1"), TOPICS);

        assertThat(retry.appended().duplicate()).isTrue();
        assertThat(retry.notified()).isTrue();
        assertThat(store.events).hasSize(1);
        assertThat(notifications.sent).containsExactly(EventNotification.of(lost.appended()));
    }

    /** Minimal store double: sequential seqs, idempotent by event id, counts appends. */
    private static final class CountingStore implements EventStore {
        final Map<String, AppendResult> events = new HashMap<>();
        int appends;
        long seq = 100;
        boolean failing;

        @Override
        public AppendResult append(TripEvent event, List<TopicId> topics) {
            if (failing) {
                throw new EventStoreUnavailableException("down (test)", null);
            }
            appends++;
            AppendResult existing = events.get(event.eventId());
            if (existing != null) {
                return new AppendResult(existing.events(), true);
            }
            List<SequencedEvent> sequenced = new ArrayList<>();
            for (TopicId topic : topics) {
                sequenced.add(new SequencedEvent(event, topic, ++seq, Instant.now()));
            }
            AppendResult result = new AppendResult(sequenced, false);
            events.put(event.eventId(), result);
            return result;
        }

        @Override
        public TopicHistory readAfter(TenantTopic topic, long afterSeq, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long openTopic(TenantTopic topic) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SequencedEvent> latestLocations(TenantTopic topic) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SequencedEvent> latestLocation(TenantTopic topic, String cabId) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FlakyPublisher implements NotificationPublisher {
        final List<EventNotification> sent = new CopyOnWriteArrayList<>();
        volatile boolean failing;
        int attempts;

        @Override
        public void publish(EventNotification notification) {
            attempts++;
            if (failing) {
                throw new NotificationPublishException(notification.eventId(), new RuntimeException("broker down"));
            }
            sent.add(notification);
        }
    }
}
