package com.moveinsync.tripupdates.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JDK WebSocket client for endpoint tests. A frozen client stops requesting frames after connecting, so TCP flow
 * control eventually blocks the server's sends to it (a frozen browser tab).
 */
final class WsTestClient implements WebSocket.Listener {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
    private final CompletableFuture<Integer> closed = new CompletableFuture<>();
    private final StringBuilder partial = new StringBuilder();
    private final boolean frozen;
    private WebSocket socket;

    private WsTestClient(boolean frozen) {
        this.frozen = frozen;
    }

    static WsTestClient connect(int port, String token) {
        return connect(port, token, false);
    }

    static WsTestClient connect(int port, String token, boolean frozen) {
        WsTestClient client = new WsTestClient(frozen);
        WebSocket.Builder builder = HTTP.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5));
        if (token != null) {
            builder.header("Authorization", "Bearer " + token);
        }
        client.socket = builder.buildAsync(URI.create("ws://localhost:" + port + "/ws"), client).join();
        return client;
    }

    void send(String json) {
        socket.sendText(json, true).join();
    }

    /** Next message, failing after 10 s. */
    JsonNode next() throws InterruptedException {
        JsonNode message = received.poll(10, TimeUnit.SECONDS);
        if (message == null) {
            throw new AssertionError("No message within 10 s");
        }
        return message;
    }

    /** Next {@code count} messages. */
    List<JsonNode> next(int count) throws InterruptedException {
        List<JsonNode> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(next());
        }
        return messages;
    }

    /** The next message, if one arrives within {@code millis}. */
    JsonNode poll(long millis) throws InterruptedException {
        return received.poll(millis, TimeUnit.MILLISECONDS);
    }

    void close() {
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
    }

    void abort() {
        socket.abort();
    }

    CompletableFuture<Integer> closed() {
        return closed;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            try {
                received.add(MAPPER.readTree(partial.toString()));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            partial.setLength(0);
        }
        if (!frozen) {
            webSocket.request(1);
        }
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        closed.complete(statusCode);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        closed.completeExceptionally(error);
    }
}
