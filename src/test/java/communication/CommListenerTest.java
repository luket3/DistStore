package communication;

import static org.junit.jupiter.api.Assertions.*;

import java.net.Socket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import testutil.Nodes;

class CommListenerTest {

    private Listener listener;
    private int port;

    @BeforeEach
    void open() throws Exception {
        port = Nodes.closedPort();
        listener = new Listener();
        listener.createSocket(port);
    }

    @AfterEach
    void close() throws Exception {
        listener.closeSocket();
    }

    @Test
    void sendsAndReceivesLengthPrefixedStrings() throws Exception {
        String payload = "héllo ✓ " + "x".repeat(100_000);
        Comm client = new Comm();
        client.createSocket("localhost", port);
        client.sendString(payload);

        Comm server = new Comm(listener.listenForConnection());
        assertEquals(payload, server.readString());

        assertEquals(port, client.getHostPort());
        assertEquals(port, server.getLclPort());
        assertEquals(client.getLclPort(), server.getHostPort());
        assertNotNull(client.getHostIp());
        assertNotNull(server.getLclIp());
        client.closeSocket();
        server.closeSocket();
    }

    @Test
    void supportsMultipleMessagesOnOneConnection() throws Exception {
        Comm client = new Comm();
        client.createSocket("localhost", port);
        client.sendString("one");
        client.sendString("two");

        Comm server = new Comm(listener.listenForConnection());
        assertEquals("one", server.readString());
        assertEquals("two", server.readString());
        client.closeSocket();
        server.closeSocket();
    }

    @Test
    void timedAcceptReturnsNullWithoutConnection() throws Exception {
        assertNull(listener.listenForConnectionWithTimeout(100));

        // still usable for a normal accept afterwards
        Socket s = new Socket("localhost", port);
        assertNotNull(listener.listenForConnectionWithTimeout(1000));
        s.close();
    }

    @Test
    void gettersHandleMissingSocket() {
        Comm comm = new Comm();
        assertNull(comm.getHostIp());
        assertNull(comm.getLclIp());
        assertEquals(-1, comm.getHostPort());
        assertEquals(-1, comm.getLclPort());
    }
}
