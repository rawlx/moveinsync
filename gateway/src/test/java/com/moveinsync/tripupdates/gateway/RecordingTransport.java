package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

/** Records sent frames. A gated transport blocks every send until permits are released (a slow or frozen client). */
public final class RecordingTransport implements ClientTransport {

    private final List<OutboundFrame> frames = new CopyOnWriteArrayList<>();
    private final Semaphore gate;
    private volatile CloseReason closeReason;

    private RecordingTransport(Semaphore gate) {
        this.gate = gate;
    }

    public static RecordingTransport open() {
        return new RecordingTransport(null);
    }

    public static RecordingTransport gated() {
        return new RecordingTransport(new Semaphore(0));
    }

    @Override
    public void send(OutboundFrame frame) throws Exception {
        if (gate != null) {
            gate.acquire();
        }
        if (closeReason != null) {
            throw new IllegalStateException("closed");
        }
        frames.add(frame);
    }

    @Override
    public void close(CloseReason reason) {
        closeReason = reason;
        if (gate != null) {
            gate.release(1_000_000); // a real close unblocks a pending write
        }
    }

    public void release(int sends) {
        gate.release(sends);
    }

    /** Every frame sent, in order. */
    public List<OutboundFrame> frames() {
        return List.copyOf(frames);
    }

    /** Control messages sent, in order. */
    public List<OutboundFrame.ControlMessage> controls() {
        return frames.stream()
                .filter(OutboundFrame.ControlFrame.class::isInstance)
                .map(frame -> ((OutboundFrame.ControlFrame) frame).message())
                .toList();
    }

    /** Events sent, in order. */
    public List<SequencedEvent> sent() {
        return frames.stream()
                .filter(OutboundFrame.EventFrame.class::isInstance)
                .map(frame -> ((OutboundFrame.EventFrame) frame).event())
                .toList();
    }

    public List<Long> seqs() {
        return sent().stream().map(SequencedEvent::seq).toList();
    }

    public List<String> eventIds() {
        return sent().stream().map(e -> e.event().eventId()).toList();
    }

    public CloseReason closeReason() {
        return closeReason;
    }
}
