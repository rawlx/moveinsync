package com.moveinsync.tripupdates.domain;

import static com.moveinsync.tripupdates.domain.TripEventTest.OCCURRED;
import static com.moveinsync.tripupdates.domain.TripEventTest.event;
import static com.moveinsync.tripupdates.domain.TripEventTest.payloadOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SequencedEventTest {

    private static final Instant PUBLISHED = Instant.parse("2026-10-29T10:00:00.120Z");

    @Test
    void sameEventOnEachTopicCarriesThatTopicsSequence() {
        TripEvent status = event(EventType.STATUS);

        SequencedEvent onTrip = new SequencedEvent(status, TopicId.trip("T-A1"), 43, PUBLISHED);
        SequencedEvent onCab = new SequencedEvent(status, TopicId.cab("C-A1"), 980, PUBLISHED);
        SequencedEvent onOffice = new SequencedEvent(status, TopicId.office("O-A1"), 5021, PUBLISHED);

        assertThat(onTrip.seq()).isEqualTo(43);
        assertThat(onCab.seq()).isEqualTo(980);
        assertThat(onOffice.seq()).isEqualTo(5021);
        assertThat(onTrip.type()).isEqualTo(EventType.STATUS);
    }

    @Test
    void tenantTopicComesFromTheEventsTenant() {
        SequencedEvent sequenced = new SequencedEvent(event(EventType.SOS), TopicId.trip("T-A1"), 1, PUBLISHED);
        assertThat(sequenced.tenantTopic()).isEqualTo(new TenantTopic("acme", TopicId.trip("T-A1")));
        assertThat(sequenced.tenantTopic()).isNotEqualTo(new TenantTopic("globex", TopicId.trip("T-A1")));
    }

    @Test
    void rejectsTripTopicOfAnotherTrip() {
        assertThatThrownBy(() -> new SequencedEvent(event(EventType.STATUS), TopicId.trip("T-B9"), 1, PUBLISHED))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("topic: trip topic does not match the event's trip");
    }

    @Test
    void rejectsCabTopicOfAnotherCab() {
        assertThatThrownBy(() -> new SequencedEvent(event(EventType.LOCATION), TopicId.cab("C-B9"), 1, PUBLISHED))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("topic: cab topic does not match the event's cab");
    }

    @Test
    void locationWithoutTripCannotBeDeliveredOnATripTopic() {
        TripEvent location = new TripEvent("evt-2", "acme", null, "C-A1", OCCURRED, payloadOf(EventType.LOCATION));

        assertThat(new SequencedEvent(location, TopicId.cab("C-A1"), 7, PUBLISHED).seq()).isEqualTo(7);
        assertThatThrownBy(() -> new SequencedEvent(location, TopicId.trip("T-A1"), 8, PUBLISHED))
                .isInstanceOf(DomainValidationException.class);
    }

    @Test
    void sosTopicCarriesSosEventsOnly() {
        assertThat(new SequencedEvent(event(EventType.SOS), TopicId.sos("O-A1"), 1, PUBLISHED).seq()).isEqualTo(1);
        assertThatThrownBy(() -> new SequencedEvent(event(EventType.STATUS), TopicId.sos("O-A1"), 1, PUBLISHED))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("topic: sos topic carries SOS events only");
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void rejectsNonPositiveSequence(long seq) {
        assertThatThrownBy(() -> new SequencedEvent(event(EventType.STATUS), TopicId.trip("T-A1"), seq, PUBLISHED))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("seq: must be >= 1");
    }

    @Test
    void rejectsMissingFields() {
        TripEvent status = event(EventType.STATUS);
        assertThatThrownBy(() -> new SequencedEvent(null, TopicId.trip("T-A1"), 1, PUBLISHED))
                .hasMessage("event: is required");
        assertThatThrownBy(() -> new SequencedEvent(status, null, 1, PUBLISHED))
                .hasMessage("topic: is required");
        assertThatThrownBy(() -> new SequencedEvent(status, TopicId.trip("T-A1"), 1, null))
                .hasMessage("publishedAt: is required");
    }
}
