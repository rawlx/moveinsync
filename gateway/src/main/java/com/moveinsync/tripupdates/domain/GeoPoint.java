package com.moveinsync.tripupdates.domain;

/**
 * A WGS-84 position. Positions can reveal where employees live, so {@link #toString()} is redacted and validation
 * messages never include the coordinates.
 */
public record GeoPoint(double lat, double lon) {

    public GeoPoint {
        if (!Double.isFinite(lat) || lat < -90 || lat > 90) {
            throw new DomainValidationException("lat", "must be a number between -90 and 90");
        }
        if (!Double.isFinite(lon) || lon < -180 || lon > 180) {
            throw new DomainValidationException("lon", "must be a number between -180 and 180");
        }
    }

    @Override
    public String toString() {
        return "GeoPoint[redacted]";
    }
}
