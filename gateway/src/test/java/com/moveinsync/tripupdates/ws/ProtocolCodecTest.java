package com.moveinsync.tripupdates.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.GatewayEvents;
import com.moveinsync.tripupdates.gateway.OutboundFrame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ProtocolCodecTest {

    private final ProtocolCodec codec = new ProtocolCodec();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void parsesEveryClientMessage() {
        assertThat(codec.parse("{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}"))
                .isEqualTo(new ClientMessage.Subscribe("m1", TopicId.trip("T-1")));
        assertThat(codec.parse("{\"type\":\"UNSUBSCRIBE\",\"id\":\"m2\",\"topic\":\"office:O-1\"}"))
                .isEqualTo(new ClientMessage.Unsubscribe("m2", TopicId.office("O-1")));
        assertThat(codec.parse("{\"type\":\"RESUME\",\"id\":\"m3\",\"topic\":\"sos:O-1\",\"lastSeq\":1761732000123004}"))
                .isEqualTo(new ClientMessage.Resume("m3", TopicId.sos("O-1"), 1761732000123004L));
        assertThat(codec.parse("{\"type\":\"ACK\",\"topic\":\"trip:T-1\",\"seq\":5}"))
                .isEqualTo(new ClientMessage.Ack(null, TopicId.trip("T-1"), 5));
        assertThat(codec.parse("{\"type\":\"RESUME\",\"id\":\"m4\",\"topic\":\"trip:T-1\",\"lastSeq\":0}"))
                .isEqualTo(new ClientMessage.Resume("m4", TopicId.trip("T-1"), 0));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "not json                                                              | BAD_REQUEST     | ",
            "[1,2]                                                                 | BAD_REQUEST     | ",
            "{\"id\":\"m1\",\"topic\":\"trip:T-1\"}                                | BAD_REQUEST     | m1",
            "{\"type\":7,\"id\":\"m1\"}                                            | BAD_REQUEST     | m1",
            "{\"type\":\"PUBLISH\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}           | UNSUPPORTED_TYPE| m1",
            "{\"type\":\"SUBSCRIBE\",\"topic\":\"trip:T-1\"}                       | BAD_REQUEST     | ",
            "{\"type\":\"SUBSCRIBE\",\"id\":\"bad id\",\"topic\":\"trip:T-1\"}     | BAD_REQUEST     | ",
            "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\"}                                | BAD_REQUEST     | m1",
            "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"bus:T-1\"}          | BAD_REQUEST     | m1",
            "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T 1\"}         | BAD_REQUEST     | m1",
            "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"tenantId\":\"globex\"} | BAD_REQUEST | m1",
            "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"role\":\"OFFICE_ADMIN\"} | BAD_REQUEST | m1",
            "{\"type\":\"RESUME\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}            | BAD_REQUEST     | m1",
            "{\"type\":\"RESUME\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"lastSeq\":-1}   | BAD_REQUEST | m1",
            "{\"type\":\"RESUME\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"lastSeq\":1.5}  | BAD_REQUEST | m1",
            "{\"type\":\"RESUME\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"lastSeq\":\"5\"}| BAD_REQUEST | m1",
            "{\"type\":\"RESUME\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"lastSeq\":99999999999999999999} | BAD_REQUEST | m1",
            "{\"type\":\"ACK\",\"topic\":\"trip:T-1\",\"seq\":0}                   | BAD_REQUEST     | ",
            "{\"type\":\"ACK\",\"id\":7,\"topic\":\"trip:T-1\",\"seq\":3}          | BAD_REQUEST     | ",
    })
    void rejectsMalformedMessagesWithACodeAndTheIdWhenReadable(String input, ErrorCode code, String id) {
        assertThatThrownBy(() -> codec.parse(input))
                .isInstanceOfSatisfying(ProtocolException.class, e -> {
                    assertThat(e.code()).isEqualTo(code);
                    assertThat(e.messageId()).isEqualTo(id);
                });
    }

    @Test
    void errorMessagesNeverEchoTheRejectedInput() {
        assertThatThrownBy(() -> codec.parse("{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:<script>\"}"))
                .message().doesNotContain("script");
    }

    @Test
    void rendersEventsWithTheStoredEventForm() throws Exception {
        var event = GatewayEvents.sequenced(GatewayEvents.status("e1"), TopicId.trip("T-1"), 42);
        JsonNode json = mapper.readTree(codec.render(new OutboundFrame.EventFrame(event)));

        assertThat(json.get("type").asText()).isEqualTo("EVENT");
        assertThat(json.get("topic").asText()).isEqualTo("trip:T-1");
        assertThat(json.get("seq").asLong()).isEqualTo(42);
        assertThat(json.get("publishedAt").asText()).isEqualTo("2026-10-29T10:00:00Z");
        assertThat(json.at("/event/eventId").asText()).isEqualTo("e1");
        assertThat(json.at("/event/type").asText()).isEqualTo("STATUS");
        assertThat(json.at("/event/data/status").asText()).isEqualTo("DRIVER_EN_ROUTE");
    }

    @Test
    void rendersControlMessages() throws Exception {
        TopicId trip = TopicId.trip("T-1");
        assertThat(render(new ServerMessage.Subscribed("m1", trip, 10)))
                .isEqualTo(mapper.readTree("{\"type\":\"SUBSCRIBED\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"seq\":10}"));
        assertThat(render(new ServerMessage.Unsubscribed("m2", trip)))
                .isEqualTo(mapper.readTree("{\"type\":\"UNSUBSCRIBED\",\"id\":\"m2\",\"topic\":\"trip:T-1\"}"));
        assertThat(render(new ServerMessage.ResumeComplete("m3", trip, 5, 9, 2))).isEqualTo(mapper.readTree(
                "{\"type\":\"RESUME_COMPLETE\",\"id\":\"m3\",\"topic\":\"trip:T-1\",\"fromSeq\":5,\"toSeq\":9,\"replayed\":2}"));
        assertThat(render(new ServerMessage.Error("m4", ErrorCode.RECOVERY_REQUIRED, "reload", trip,
                new ServerMessage.Recovery(1, 5, 9)))).isEqualTo(mapper.readTree(
                "{\"type\":\"ERROR\",\"id\":\"m4\",\"code\":\"RECOVERY_REQUIRED\",\"message\":\"reload\","
                        + "\"topic\":\"trip:T-1\",\"recovery\":{\"requestedSeq\":1,\"oldestAvailableAfterSeq\":5,\"headSeq\":9}}"));
        assertThat(render(ServerMessage.Error.of(null, ErrorCode.BAD_REQUEST, "bad", null)))
                .isEqualTo(mapper.readTree("{\"type\":\"ERROR\",\"code\":\"BAD_REQUEST\",\"message\":\"bad\"}"));
    }

    private JsonNode render(ServerMessage message) throws Exception {
        return mapper.readTree(codec.render(new OutboundFrame.ControlFrame(message)));
    }
}
