package com.moveinsync.tripupdates.auth;

import java.util.Optional;

/**
 * Establishes the identity of a connecting client from a bearer credential, once, at the WebSocket handshake.
 * Implementations must derive tenant, role and permissions from trusted server-side data (a verified token or an
 * identity provider), never from anything else the client sends.
 */
public interface ConnectionAuthenticator {

    /** @return the identity, or empty if the credential is missing, unknown, expired or invalid */
    Optional<Identity> authenticate(String bearerToken);
}
