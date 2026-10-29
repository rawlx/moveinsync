package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.Identifiers;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One client connection: its tenant, its {@link ClientOutbox}, and a dedicated virtual-thread writer that is the
 * only caller of {@link ClientTransport#send}. Producers (topic feeds) only ever call {@link #offer}, which never
 * blocks, so a slow or frozen client cannot slow down feeds or other clients.
 */
public final class ClientSession {

    private static final Logger log = LoggerFactory.getLogger(ClientSession.class);

    private final String id;
    private final String tenantId;
    private final ClientOutbox outbox;
    private final ClientTransport transport;
    private final Consumer<ClientSession> onClosed;
    private final AtomicReference<CloseReason> closeReason = new AtomicReference<>();

    ClientSession(String id, String tenantId, ClientOutbox outbox, ClientTransport transport,
            Consumer<ClientSession> onClosed) {
        this.id = Identifiers.require(id, "sessionId");
        this.tenantId = Identifiers.require(tenantId, "tenantId");
        this.outbox = outbox;
        this.transport = transport;
        this.onClosed = onClosed;
    }

    public String id() {
        return id;
    }

    public String tenantId() {
        return tenantId;
    }

    public ClientOutbox outbox() {
        return outbox;
    }

    public boolean isClosed() {
        return closeReason.get() != null;
    }

    /** The reason the session was closed, or null while open. */
    public CloseReason closeReason() {
        return closeReason.get();
    }

    void startWriter() {
        Thread.ofVirtual().name("client-writer-" + id).start(this::writeLoop);
    }

    /** Queues an event for this client. Never blocks; overflowing the reliable lane closes the session. */
    void offer(SequencedEvent event) {
        if (outbox.offer(event) == ClientOutbox.OfferResult.OVERFLOW) {
            close(CloseReason.SLOW_CONSUMER);
        }
    }

    /**
     * Closes the session once (later calls are ignored). Closing the transport and unregistering subscriptions run
     * on a separate virtual thread: the caller may hold a feed lock, and the transport may be slow to close.
     */
    public void close(CloseReason reason) {
        if (!closeReason.compareAndSet(null, reason)) {
            return;
        }
        outbox.close();
        Thread.ofVirtual().name("client-close-" + id).start(() -> {
            try {
                transport.close(reason);
            } catch (RuntimeException e) {
                log.debug("Closing transport of session {} failed", id, e);
            } finally {
                onClosed.accept(this);
            }
        });
    }

    private void writeLoop() {
        try {
            SequencedEvent next;
            while ((next = outbox.take()) != null) {
                transport.send(next);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close(CloseReason.SHUTDOWN);
        } catch (Exception e) {
            log.debug("Send to session {} failed", id, e);
            close(CloseReason.TRANSPORT_ERROR);
        }
    }

    @Override
    public String toString() {
        return "ClientSession[" + id + ", tenant=" + tenantId + "]";
    }
}
