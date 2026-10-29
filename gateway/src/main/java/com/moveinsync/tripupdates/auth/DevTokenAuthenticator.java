package com.moveinsync.tripupdates.auth;

import java.util.Map;
import java.util.Optional;

/**
 * DEVELOPMENT AND TEST ONLY. Maps opaque dev tokens to identities defined in server-side fixtures. The token carries
 * no claims: tenant, role and permissions come only from the fixture. It offers no real security (tokens are
 * well-known strings, never expire, are not signed) and is enabled only with {@code app.auth.mode=dev}. Production
 * needs a {@link ConnectionAuthenticator} backed by a verified token (e.g. a signed JWT) or an identity provider.
 */
public final class DevTokenAuthenticator implements ConnectionAuthenticator {

    private final Map<String, Identity> identitiesByToken;

    public DevTokenAuthenticator(Map<String, Identity> identitiesByToken) {
        this.identitiesByToken = Map.copyOf(identitiesByToken);
    }

    @Override
    public Optional<Identity> authenticate(String bearerToken) {
        return bearerToken == null ? Optional.empty() : Optional.ofNullable(identitiesByToken.get(bearerToken));
    }
}
