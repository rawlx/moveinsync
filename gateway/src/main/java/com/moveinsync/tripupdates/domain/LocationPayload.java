package com.moveinsync.tripupdates.domain;

/** Cab position. {@code speedKph} and {@code heading} are optional (null when the device does not report them). */
public record LocationPayload(GeoPoint position, Double speedKph, Double heading) implements EventPayload {

    static final double MAX_SPEED_KPH = 300;

    public LocationPayload {
        if (position == null) {
            throw new DomainValidationException("data.position", "is required");
        }
        if (speedKph != null && (!Double.isFinite(speedKph) || speedKph < 0 || speedKph > MAX_SPEED_KPH)) {
            throw new DomainValidationException("data.speedKph", "must be between 0 and " + (int) MAX_SPEED_KPH);
        }
        if (heading != null && (!Double.isFinite(heading) || heading < 0 || heading >= 360)) {
            throw new DomainValidationException("data.heading", "must be in [0, 360)");
        }
    }

    @Override
    public EventType type() {
        return EventType.LOCATION;
    }

    @Override
    public String toString() {
        return "LocationPayload[redacted]";
    }
}
