package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.TopicAuthorizer;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.AttachOutcome;
import com.moveinsync.tripupdates.gateway.ClientSession;
import com.moveinsync.tripupdates.gateway.GatewayMetrics;
import com.moveinsync.tripupdates.gateway.Subscription;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.store.EventStoreException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles client messages of one authenticated connection; independent of the WebSocket API. Every reply, including
 * errors, is queued on the client's session so it stays ordered with events (see {@link ProtocolCodec} for the
 * protocol). Authorization is checked on every SUBSCRIBE and RESUME with the server-side {@link Identity}; nothing in
 * a message can change tenant, role or permissions.
 */
public final class ProtocolService {

    private static final Logger log = LoggerFactory.getLogger(ProtocolService.class);
    private static final String NOT_PERMITTED = "Subscription not permitted";

    /**
     * An authenticated connection: who it is, where its frames go, and when the client was last heard from
     * (any text frame or pong), which drives the idle timeout.
     */
    public static final class Connection {

        private final Identity identity;
        private final ClientSession session;
        private final AtomicLong lastInboundMillis = new AtomicLong(System.currentTimeMillis());

        public Connection(Identity identity, ClientSession session) {
            this.identity = identity;
            this.session = session;
        }

        public Identity identity() {
            return identity;
        }

        public ClientSession session() {
            return session;
        }

        /** Records inbound activity from the client. */
        public void touch() {
            touchAt(System.currentTimeMillis());
        }

        void touchAt(long millis) {
            lastInboundMillis.set(millis);
        }

        public long lastInboundMillis() {
            return lastInboundMillis.get();
        }
    }

    private final SubscriptionRegistry registry;
    private final TopicAuthorizer authorizer;
    private final ProtocolCodec codec;
    private final int maxMessageChars;
    private final int maxSubscriptions;
    private final GatewayMetrics metrics;

    public ProtocolService(SubscriptionRegistry registry, TopicAuthorizer authorizer, ProtocolCodec codec,
            int maxMessageChars, int maxSubscriptions) {
        this(registry, authorizer, codec, maxMessageChars, maxSubscriptions, GatewayMetrics.NOOP);
    }

    public ProtocolService(SubscriptionRegistry registry, TopicAuthorizer authorizer, ProtocolCodec codec,
            int maxMessageChars, int maxSubscriptions, GatewayMetrics metrics) {
        this.registry = registry;
        this.authorizer = authorizer;
        this.codec = codec;
        this.maxMessageChars = maxMessageChars;
        this.maxSubscriptions = maxSubscriptions;
        this.metrics = metrics;
    }

    /** Handles one text frame. Never throws for client mistakes; replies with {@code ERROR} instead. */
    public void handle(Connection connection, String text) {
        connection.touch();
        if (connection.session().isClosed()) {
            return;
        }
        if (text.length() > maxMessageChars) {
            reply(connection, ServerMessage.Error.of(null, ErrorCode.MESSAGE_TOO_LARGE,
                    "Message exceeds " + maxMessageChars + " characters", null));
            return;
        }
        ClientMessage message;
        try {
            message = codec.parse(text);
        } catch (ProtocolException e) {
            reply(connection, ServerMessage.Error.of(e.messageId(), e.code(), e.getMessage(), null));
            return;
        }
        try {
            switch (message) {
                case ClientMessage.Subscribe subscribe -> attach(connection, subscribe, null,
                        attached -> new ServerMessage.Subscribed(subscribe.id(), subscribe.topic(), attached.toSeq()));
                case ClientMessage.Resume resume -> attach(connection, resume, resume.lastSeq(),
                        attached -> new ServerMessage.ResumeComplete(resume.id(), resume.topic(), attached.fromSeq(),
                                attached.toSeq(), attached.replayed()));
                case ClientMessage.Unsubscribe unsubscribe -> unsubscribe(connection, unsubscribe);
                case ClientMessage.Ack ack -> acknowledge(connection, ack);
            }
        } catch (EventStoreException e) {
            if (message instanceof ClientMessage.Subscribe || message instanceof ClientMessage.Resume) {
                metrics.subscriptionRequest(message instanceof ClientMessage.Resume,
                        GatewayMetrics.SubscriptionOutcome.UNAVAILABLE);
            }
            log.warn("Event store unavailable while handling {} for session {}", message.getClass().getSimpleName(),
                    connection.session().id(), e);
            reply(connection, ServerMessage.Error.of(message.id(), ErrorCode.UNAVAILABLE,
                    "Temporarily unavailable, retry later", message.topic()));
        } catch (IllegalStateException e) {
            // The session closed concurrently (disconnect, slow consumer); nothing to reply to.
            log.debug("Session {} closed while handling a message", connection.session().id());
        }
    }

    private void attach(Connection connection, ClientMessage message, Long resumeAfter,
            Function<AttachOutcome.Attached, ServerMessage> confirmation) {
        TopicId topic = message.topic();
        boolean resume = resumeAfter != null;
        if (!authorizer.mayRead(connection.identity(), topic)) {
            metrics.subscriptionRequest(resume, GatewayMetrics.SubscriptionOutcome.FORBIDDEN);
            log.info("Denied {} of {} to {} ({})", message.getClass().getSimpleName(), topic,
                    connection.identity().subject(), connection.identity().tenantId());
            reply(connection, ServerMessage.Error.of(message.id(), ErrorCode.FORBIDDEN, NOT_PERMITTED, topic));
            return;
        }
        AttachOutcome outcome = registry.attach(connection.session(), topic, resumeAfter, maxSubscriptions,
                confirmation::apply);
        metrics.subscriptionRequest(resume, switch (outcome) {
            case AttachOutcome.Attached attached -> GatewayMetrics.SubscriptionOutcome.ATTACHED;
            case AttachOutcome.AlreadySubscribed already -> GatewayMetrics.SubscriptionOutcome.ALREADY_SUBSCRIBED;
            case AttachOutcome.LimitReached limit -> GatewayMetrics.SubscriptionOutcome.LIMIT_REACHED;
            case AttachOutcome.HistoryUnavailable unavailable -> GatewayMetrics.SubscriptionOutcome.RECOVERY_REQUIRED;
        });
        if (outcome instanceof AttachOutcome.HistoryUnavailable) {
            log.info("RESUME of {} for session {} needs recovery: position outside the retained history", topic,
                    connection.session().id());
        }
        switch (outcome) {
            case AttachOutcome.Attached attached -> {
                // Confirmation already queued, in order, by the feed.
            }
            case AttachOutcome.AlreadySubscribed already -> reply(connection, ServerMessage.Error.of(message.id(),
                    ErrorCode.ALREADY_SUBSCRIBED, "Already subscribed; UNSUBSCRIBE first to resume", topic));
            case AttachOutcome.LimitReached limit -> reply(connection, ServerMessage.Error.of(message.id(),
                    ErrorCode.TOO_MANY_SUBSCRIPTIONS, "At most " + limit.limit() + " subscriptions per connection",
                    topic));
            case AttachOutcome.HistoryUnavailable unavailable -> reply(connection, new ServerMessage.Error(
                    message.id(), ErrorCode.RECOVERY_REQUIRED,
                    "Requested position is not in the retained history; reload state and SUBSCRIBE", topic,
                    new ServerMessage.Recovery(unavailable.requestedSeq(), unavailable.floorSeq(),
                            unavailable.headSeq())));
        }
    }

    /**
     * Re-checks every live subscription of the connection against the current directory (bounded authorization
     * refresh, run periodically). A subscription that is no longer permitted, e.g. after a trip was reassigned, is
     * removed together with its pending events, and the client gets {@code ERROR SUBSCRIPTION_REVOKED}.
     *
     * @return the number of subscriptions revoked
     */
    public int revalidate(Connection connection) {
        int revoked = 0;
        for (TopicId topic : registry.topicsOf(connection.session())) {
            if (!authorizer.mayRead(connection.identity(), topic) && registry.unsubscribe(connection.session(), topic)) {
                revoked++;
                metrics.subscriptionRevoked();
                log.info("Revoked subscription of session {} to {}: no longer permitted", connection.session().id(),
                        topic);
                reply(connection, ServerMessage.Error.of(null, ErrorCode.SUBSCRIPTION_REVOKED,
                        "Subscription no longer permitted", topic));
            }
        }
        return revoked;
    }

    private void unsubscribe(Connection connection, ClientMessage.Unsubscribe unsubscribe) {
        if (registry.unsubscribe(connection.session(), unsubscribe.topic())) {
            reply(connection, new ServerMessage.Unsubscribed(unsubscribe.id(), unsubscribe.topic()));
        } else {
            reply(connection, ServerMessage.Error.of(unsubscribe.id(), ErrorCode.NOT_SUBSCRIBED, "Not subscribed",
                    unsubscribe.topic()));
        }
    }

    private void acknowledge(Connection connection, ClientMessage.Ack ack) {
        Subscription subscription = registry.subscription(connection.session(), ack.topic());
        if (subscription == null) {
            reply(connection, ServerMessage.Error.of(ack.id(), ErrorCode.NOT_SUBSCRIBED, "Not subscribed",
                    ack.topic()));
        } else if (!subscription.acknowledge(ack.seq())) {
            reply(connection, ServerMessage.Error.of(ack.id(), ErrorCode.INVALID_ACK,
                    "seq was not delivered on this subscription", ack.topic()));
        }
        // A valid ACK (including a repeated one) needs no reply.
    }

    private static void reply(Connection connection, ServerMessage message) {
        connection.session().offerControl(message);
    }
}
