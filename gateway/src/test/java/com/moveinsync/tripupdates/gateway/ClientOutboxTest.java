package com.moveinsync.tripupdates.gateway;

import static com.moveinsync.tripupdates.gateway.GatewayEvents.CAB;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.OFFICE;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.TRIP;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.location;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.sequenced;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.sos;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.status;
import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.ClientOutbox.OfferResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ClientOutboxTest {

    private final ClientOutbox outbox = new ClientOutbox(4, 3);

    @Test
    void reliableEventsKeepFifoOrder() throws InterruptedException {
        for (int i = 1; i <= 4; i++) {
            assertThat(outbox.offer(sequenced(status("e" + i), TRIP, i))).isEqualTo(OfferResult.QUEUED);
        }
        assertThat(drain(4)).extracting(SequencedEvent::seq).containsExactly(1L, 2L, 3L, 4L);
    }

    @Test
    void pendingLocationIsConflatedPerCab() throws InterruptedException {
        assertThat(outbox.offer(sequenced(location("l1", "C-1", 1), OFFICE, 1))).isEqualTo(OfferResult.QUEUED);
        assertThat(outbox.offer(sequenced(location("l2", "C-2", 2), OFFICE, 2))).isEqualTo(OfferResult.QUEUED);
        assertThat(outbox.offer(sequenced(location("l3", "C-1", 3), OFFICE, 3))).isEqualTo(OfferResult.CONFLATED);

        assertThat(outbox.locationDepth()).isEqualTo(2);
        assertThat(outbox.conflatedCount()).isEqualTo(1);
        assertThat(drain(2)).extracting(e -> e.event().eventId()).containsExactly("l2", "l3");
    }

    @Test
    void sameCabOnDifferentTopicsHasSeparateSlots() {
        outbox.offer(sequenced(location("l1", "C-1", 1), CAB, 1));
        outbox.offer(sequenced(location("l1", "C-1", 1), OFFICE, 7));
        assertThat(outbox.locationDepth()).isEqualTo(2);
    }

    @Test
    void olderLocationNeverReplacesANewerPendingOne() throws InterruptedException {
        outbox.offer(sequenced(location("new", "C-1", 1), OFFICE, 9));
        assertThat(outbox.offer(sequenced(location("old", "C-1", 1), OFFICE, 5))).isEqualTo(OfferResult.CONFLATED);
        assertThat(drain(1)).extracting(e -> e.event().eventId()).containsExactly("new");
    }

    @Test
    void locationLaneIsBoundedByEvictingTheOldestCab() throws InterruptedException {
        for (int cab = 1; cab <= 3; cab++) {
            outbox.offer(sequenced(location("l" + cab, "C-" + cab, 1), OFFICE, cab));
        }
        assertThat(outbox.offer(sequenced(location("l4", "C-4", 1), OFFICE, 4)))
                .isEqualTo(OfferResult.LOCATION_EVICTED);
        assertThat(outbox.locationDepth()).isEqualTo(3);
        assertThat(outbox.evictedCount()).isEqualTo(1);
        assertThat(drain(3)).extracting(e -> e.event().cabId()).containsExactly("C-2", "C-3", "C-4");
    }

    @Test
    void sosAndStatusAreServedBeforePendingLocationsAndKeepTheirOrder() throws InterruptedException {
        outbox.offer(sequenced(location("l1", "C-1", 1), OFFICE, 1));
        outbox.offer(sequenced(location("l2", "C-2", 1), OFFICE, 2));
        outbox.offer(sequenced(status("s1"), OFFICE, 3));
        outbox.offer(sequenced(location("l3", "C-3", 1), OFFICE, 4));
        outbox.offer(sequenced(sos("sos1"), OFFICE, 5));

        assertThat(drain(5)).extracting(e -> e.event().eventId()).containsExactly("s1", "sos1", "l1", "l2", "l3");
    }

    @Test
    void controlMessagesShareTheReliableLaneInOrderAndCountTowardsItsCapacity() throws InterruptedException {
        OutboundFrame.ControlMessage confirmed = new OutboundFrame.ControlMessage() { };
        outbox.offer(sequenced(location("l1", "C-1", 1), OFFICE, 1));
        outbox.offer(sequenced(status("e1"), TRIP, 2));
        assertThat(outbox.offerControl(confirmed)).isEqualTo(OfferResult.QUEUED);
        outbox.offer(sequenced(status("e2"), TRIP, 3));

        assertThat(outbox.take()).isEqualTo(new OutboundFrame.EventFrame(sequenced(status("e1"), TRIP, 2)));
        assertThat(outbox.take()).isEqualTo(new OutboundFrame.ControlFrame(confirmed));
        assertThat(event(outbox.take()).event().eventId()).isEqualTo("e2");
        assertThat(event(outbox.take()).event().eventId()).isEqualTo("l1");

        for (int i = 0; i < 4; i++) {
            outbox.offerControl(confirmed);
        }
        assertThat(outbox.offerControl(confirmed)).isEqualTo(OfferResult.OVERFLOW);
        assertThat(outbox.isClosed()).isTrue();
    }

    @Test
    void discardTopicRemovesOnlyThatTopicsPendingEventsAndKeepsControls() throws InterruptedException {
        OutboundFrame.ControlMessage confirmed = new OutboundFrame.ControlMessage() { };
        outbox.offer(sequenced(status("trip-1"), TRIP, 1));
        outbox.offer(sequenced(status("office-1"), OFFICE, 2));
        outbox.offer(sequenced(location("trip-loc", "C-1", 1), CAB, 3));
        outbox.offer(sequenced(location("office-loc", "C-1", 1), OFFICE, 4));
        outbox.offerControl(confirmed);

        assertThat(outbox.discardTopic(OFFICE)).isEqualTo(2);

        assertThat(outbox.take()).isEqualTo(new OutboundFrame.EventFrame(sequenced(status("trip-1"), TRIP, 1)));
        assertThat(outbox.take()).isEqualTo(new OutboundFrame.ControlFrame(confirmed));
        assertThat(event(outbox.take()).event().eventId()).isEqualTo("trip-loc");
        assertThat(outbox.reliableDepth() + outbox.locationDepth()).isZero();
        assertThat(outbox.discardTopic(OFFICE)).isZero();
    }

    @Test
    void reliableOverflowClosesTheOutboxInsteadOfDroppingSilently() throws InterruptedException {
        for (int i = 1; i <= 4; i++) {
            outbox.offer(sequenced(status("e" + i), TRIP, i));
        }
        assertThat(outbox.offer(sequenced(status("e5"), TRIP, 5))).isEqualTo(OfferResult.OVERFLOW);

        assertThat(outbox.isClosed()).isTrue();
        assertThat(outbox.reliableDepth()).isZero();
        assertThat(outbox.take()).isNull();
        assertThat(outbox.offer(sequenced(status("e6"), TRIP, 6))).isEqualTo(OfferResult.CLOSED);
        assertThat(outbox.offer(sequenced(location("l1", "C-1", 1), OFFICE, 7))).isEqualTo(OfferResult.CLOSED);
    }

    @Test
    void locationsNeverOverflowTheReliableLane() {
        for (int i = 1; i <= 100; i++) {
            assertThat(outbox.offer(sequenced(location("l" + i, "C-" + (i % 5), 1), OFFICE, i)))
                    .isNotEqualTo(OfferResult.OVERFLOW);
        }
        assertThat(outbox.isClosed()).isFalse();
        assertThat(outbox.locationDepth()).isEqualTo(3);
    }

    @Test
    void takeBlocksUntilAnEventArrivesAndCloseReleasesIt() throws Exception {
        CountDownLatch taken = new CountDownLatch(1);
        List<SequencedEvent> got = new ArrayList<>();
        Thread consumer = Thread.ofVirtual().start(() -> {
            try {
                got.add(event(outbox.take()));
                taken.countDown();
                got.add(event(outbox.take()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(taken.await(200, TimeUnit.MILLISECONDS)).isFalse();
        outbox.offer(sequenced(status("e1"), TRIP, 1));
        assertThat(taken.await(5, TimeUnit.SECONDS)).isTrue();

        outbox.close();
        consumer.join(5_000);
        assertThat(consumer.isAlive()).isFalse();
        assertThat(got).hasSize(2);
        assertThat(got.get(1)).isNull();
    }

    @Test
    void concurrentProducersKeepPerTopicOrder() throws Exception {
        ClientOutbox big = new ClientOutbox(100_000, 16);
        int producers = 4;
        int perProducer = 5_000;
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            TopicId topic = TopicId.office("O-" + p);
            threads.add(Thread.ofPlatform().start(() -> {
                for (int i = 1; i <= perProducer; i++) {
                    big.offer(sequenced(status("e" + i), topic, i));
                }
            }));
        }
        Map<TopicId, Long> last = new HashMap<>();
        for (int i = 0; i < producers * perProducer; i++) {
            SequencedEvent next = event(big.take());
            Long previous = last.put(next.topic(), next.seq());
            assertThat(next.seq()).isEqualTo(previous == null ? 1 : previous + 1);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(last.values()).containsOnly((long) perProducer);
    }

    private static SequencedEvent event(OutboundFrame frame) {
        return frame == null ? null : ((OutboundFrame.EventFrame) frame).event();
    }

    private List<SequencedEvent> drain(int count) throws InterruptedException {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(event(outbox.take()));
        }
        assertThat(outbox.reliableDepth() + outbox.locationDepth()).isZero();
        return out;
    }
}
