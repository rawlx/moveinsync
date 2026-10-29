package com.moveinsync.tripupdates.store;

/**
 * Redis replied with an error (for example out of memory under {@code noeviction}, or inconsistent topic state
 * detected by the script's pre-checks). The scripts check everything before their first write, so nothing was
 * written.
 */
public final class AppendRejectedException extends EventStoreException {

    public AppendRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
