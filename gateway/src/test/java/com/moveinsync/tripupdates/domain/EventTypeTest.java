package com.moveinsync.tripupdates.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class EventTypeTest {

    @Test
    void onlyLocationIsConflatable() {
        assertThat(EventType.LOCATION.deliveryClass()).isEqualTo(DeliveryClass.LATEST_VALUE);
        assertThat(EventType.LOCATION.isConflatable()).isTrue();
        for (EventType type : EnumSet.of(EventType.STATUS, EventType.DELAY, EventType.SOS)) {
            assertThat(type.deliveryClass()).isEqualTo(DeliveryClass.ORDERED);
            assertThat(type.isConflatable()).isFalse();
        }
    }

    @Test
    void onlySosIsPriority() {
        assertThat(EnumSet.allOf(EventType.class)).filteredOn(EventType::isPriority).containsExactly(EventType.SOS);
    }

    @Test
    void everyTypeExceptLocationRequiresATrip() {
        assertThat(EnumSet.allOf(EventType.class)).filteredOn(t -> !t.requiresTrip())
                .containsExactly(EventType.LOCATION);
    }

    @Test
    void terminalTripStatuses() {
        assertThat(EnumSet.allOf(TripStatus.class)).filteredOn(TripStatus::isTerminal)
                .containsExactlyInAnyOrder(TripStatus.COMPLETED, TripStatus.CANCELLED);
    }
}
