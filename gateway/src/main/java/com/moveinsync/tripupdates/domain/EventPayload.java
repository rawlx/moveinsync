package com.moveinsync.tripupdates.domain;

/**
 * Type-specific event content. The event type is derived from the payload class, so an event can never carry a
 * payload that disagrees with its type.
 */
public sealed interface EventPayload
        permits LocationPayload, StatusPayload, DelayPayload, SosPayload {

    EventType type();
}
