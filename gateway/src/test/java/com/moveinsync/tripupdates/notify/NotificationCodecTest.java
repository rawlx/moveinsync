package com.moveinsync.tripupdates.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.TopicId;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class NotificationCodecTest {

    private final NotificationCodec codec = new NotificationCodec();

    @Test
    void roundTripsWithTenantTopicsSequencesEventIdAndType() {
        EventNotification notification = new EventNotification("acme", "evt-1", EventType.SOS, "C-1", List.of(
                new EventNotification.TopicSeq(TopicId.trip("T-1"), 1761732000123001L),
                new EventNotification.TopicSeq(TopicId.office("O-1"), 42)));

        String encoded = codec.encode(notification);

        assertThat(encoded).isEqualTo("v1|acme|evt-1|SOS|C-1|trip:T-1=1761732000123001,office:O-1=42");
        assertThat(codec.decode(encoded)).isEqualTo(notification);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {
            "",
            "v2|acme|evt-1|SOS|C-1|trip:T-1=1",
            "v1|acme|evt-1|SOS|C-1",
            "v1|acme|evt-1|TELEPORT|C-1|trip:T-1=1",
            "v1|acme|evt-1|SOS|C-1|trip:T-1",
            "v1|acme|evt-1|SOS|C-1|trip:T-1=x",
            "v1|acme|evt-1|SOS|C-1|trip:T-1=0",
            "v1|acme|evt-1|SOS|C-1|bus:T-1=1",
            "v1|ac me|evt-1|SOS|C-1|trip:T-1=1",
            "v1|acme|evt-1|SOS|C-1|",
            "v1|acme|evt-1|SOS|C-1|trip:T-1=1|extra",
    })
    void rejectsMalformedMessages(String message) {
        assertThatThrownBy(() -> codec.decode(message)).isInstanceOf(DomainValidationException.class);
    }
}
