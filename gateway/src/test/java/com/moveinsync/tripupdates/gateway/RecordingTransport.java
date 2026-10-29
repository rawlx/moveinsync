package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

/** Records sent events. A gated transport blocks every send until permits are released (a slow or frozen client). */
final class RecordingTransport implements ClientTransport {

    private final List<SequencedEvent> sent = new CopyOnWriteArrayList<>();
    private final Semaphore gate;
    private volatile CloseReason closeReason;

    private RecordingTransport(Semaphore gate) {
        this.gate = gate;
    }

    static RecordingTransport open() {
        return new RecordingTransport(null);
    }

    static RecordingTransport gated() {
        return new RecordingTransport(new Semaphore(0));
    }

    @Override
    public void send(SequencedEvent event) throws Exception {
        if (gate != null) {
            gate.acquire();
        }
        if (closeReason != null) {
            throw new IllegalStateException("closed");
        }
        sent.add(event);
    }

    @Override
    public void close(CloseReason reason) {
        closeReason = reason;
        if (gate != null) {
            gate.release(1_000_000); // a real close unblocks a pending write
        }
    }

    void release(int sends) {
        gate.release(sends);
    }

    List<SequencedEvent> sent() {
        return List.copyOf(sent);
    }

    List<Long> seqs() {
        return sent.stream().map(SequencedEvent::seq).toList();
    }

    List<String> eventIds() {
        return sent.stream().map(e -> e.event().eventId()).toList();
    }

    CloseReason closeReason() {
        return closeReason;
    }
}
