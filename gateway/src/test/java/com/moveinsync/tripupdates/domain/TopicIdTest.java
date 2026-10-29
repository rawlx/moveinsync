package com.moveinsync.tripupdates.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class TopicIdTest {

    @Test
    void parsesEachTopicKind() {
        assertThat(TopicId.parse("trip:T-A1")).isEqualTo(TopicId.trip("T-A1"));
        assertThat(TopicId.parse("cab:C_17")).isEqualTo(TopicId.cab("C_17"));
        assertThat(TopicId.parse("office:O1")).isEqualTo(TopicId.office("O1"));
        assertThat(TopicId.parse("sos:O1")).isEqualTo(TopicId.sos("O1"));
    }

    @Test
    void nameRoundTrips() {
        TopicId topic = TopicId.office("O-A1");
        assertThat(topic.name()).isEqualTo("office:O-A1");
        assertThat(topic.toString()).isEqualTo("office:O-A1");
        assertThat(TopicId.parse(topic.name())).isEqualTo(topic);
    }

    @Test
    void acceptsIdOfMaximumLength() {
        String id = "a".repeat(Identifiers.MAX_LENGTH);
        assertThat(TopicId.parse("trip:" + id).id()).isEqualTo(id);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            "trip",            // no separator
            "trip:",           // empty id
            ":T-A1",           // empty kind
            "bus:T-A1",        // unknown kind
            "TRIP:T-A1",       // kinds are lower-case
            "Trip:T-A1",
            " trip:T-A1",      // surrounding whitespace
            "trip:T-A1 ",
            "trip:T A1",       // invalid id characters
            "trip:T/A1",
            "trip:T:A1",       // second separator
            "trip:T\"A1",
            "trip:T*",
            "trip:tríp",  // non-ASCII
    })
    void rejectsMalformedNames(String name) {
        assertThatThrownBy(() -> TopicId.parse(name)).isInstanceOf(DomainValidationException.class);
    }

    @Test
    void rejectsIdLongerThanMaximum() {
        String name = "cab:" + "a".repeat(Identifiers.MAX_LENGTH + 1);
        assertThatThrownBy(() -> TopicId.parse(name))
                .isInstanceOf(DomainValidationException.class)
                .extracting(e -> ((DomainValidationException) e).field())
                .isEqualTo("topic.id");
    }

    @Test
    void rejectsNullKind() {
        assertThatThrownBy(() -> new TopicId(null, "T-A1"))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("topic.kind: is required");
    }

    @Test
    void errorMessageDoesNotEchoTheRejectedValue() {
        String hostile = "trip:<script>alert(1)</script>";
        assertThatThrownBy(() -> TopicId.parse(hostile))
                .isInstanceOf(DomainValidationException.class)
                .message()
                .doesNotContain("script");
    }
}
