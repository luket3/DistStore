package testutil;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import cluster.Node;
import communication.Comm;
import message.Message;
import message.MessageDeserializer;

/**
 * A real TCP endpoint that records every message sent to it, so tests can
 * assert on what a component actually put on the wire.
 */
public class Capture implements AutoCloseable {
    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Message.class, new MessageDeserializer())
            .create();

    private final ServerSocket server;
    private final LinkedBlockingQueue<Message> received = new LinkedBlockingQueue<>();
    public final int port;

    public Capture() throws IOException {
        server = new ServerSocket(0);
        port = server.getLocalPort();
        Thread t = new Thread(this::loop, "Capture-" + port);
        t.setDaemon(true);
        t.start();
    }

    private void loop() {
        while (!server.isClosed()) {
            try (Socket s = server.accept()) {
                received.add(GSON.fromJson(new Comm(s).readString(), Message.class));
            } catch (Exception e) {
                // closed or malformed; keep going until closed
            }
        }
    }

    /** A node whose address is this capture endpoint. */
    public Node node(String id) {
        return new Node(id, "localhost", port);
    }

    /** Next message received within the timeout, or null. */
    public Message next(long timeoutMs) throws InterruptedException {
        return received.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Next message of the given type within the timeout, skipping others, or null. */
    @SuppressWarnings("unchecked")
    public <T extends Message> T next(Class<T> type, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            long left = deadline - System.currentTimeMillis();
            if (left <= 0)
                return null;
            Message m = received.poll(left, TimeUnit.MILLISECONDS);
            if (m == null)
                return null;
            if (type.isInstance(m))
                return (T) m;
        }
    }

    /** Discards everything received so far. */
    public void clear() {
        received.clear();
    }

    /** Every message received until none arrives for quietMs. */
    public List<Message> drain(long quietMs) throws InterruptedException {
        List<Message> all = new ArrayList<>();
        Message m;
        while ((m = next(quietMs)) != null)
            all.add(m);
        return all;
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
