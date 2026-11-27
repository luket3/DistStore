package endpoints;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import cluster.Node;
import communication.HandOff;
import message.DictMsg;
import message.Message;
import message.NodeMsg;
import message.Response;
import testutil.Capture;
import testutil.Nodes;

class ClientImpTest {

    @Test
    void buildsDictionaryMessages() throws Exception {
        ClientImp client = new ClientImp(Nodes.closedPort());

        DictMsg put = (DictMsg) client.buildMessage("Put k v");
        assertEquals("Put", put.action);
        assertEquals("k", put.key);
        assertEquals("v", put.value);
        assertNotNull(put.client);

        DictMsg get = (DictMsg) client.buildMessage("Get k");
        assertNull(get.value);
        assertEquals("Delete", ((DictMsg) client.buildMessage("Delete k")).action);
    }

    @Test
    void buildsMembershipMessages() throws Exception {
        ClientImp client = new ClientImp(Nodes.closedPort());
        NodeMsg add = (NodeMsg) client.buildMessage("Add N7");
        assertEquals("Add", add.action);
        assertEquals("N7", add.node.id);
        assertEquals("Remove", ((NodeMsg) client.buildMessage("Remove N7")).action);
    }

    @Test
    void rejectsMalformedQueries() throws Exception {
        ClientImp client = new ClientImp(Nodes.closedPort());
        assertNull(client.buildMessage("Put k"));
        assertNull(client.buildMessage("Get"));
        assertNull(client.buildMessage("Explode k"));
        assertNull(client.buildMessage("Add"));
    }

    @Test
    void updateMessageStampsTheClientVersion() throws Exception {
        ClientImp client = new ClientImp(Nodes.closedPort());
        Message msg = new DictMsg("Get", "k", null, 99);
        assertEquals(-1, client.updateMessage(msg).version); // no cluster installed yet
        assertNull(client.updateMessage(new Response("x")));
    }

    @Test
    void receivesResponsesOnItsListener() throws Exception {
        int port = Nodes.closedPort();
        ClientImp client = new ClientImp(port);
        client.startListener();

        // the listener starts asynchronously; retry until it accepts
        String json = Capture.GSON.toJson(new Response("hello"));
        long deadline = System.currentTimeMillis() + 3000;
        while (true) {
            try {
                HandOff.sendToNode(new Node("client", "localhost", port), json, "logs/test.log");
                break;
            } catch (RuntimeException e) {
                if (System.currentTimeMillis() > deadline) throw e;
                Thread.sleep(50);
            }
        }
        assertEquals("hello", client.getResponse());
    }
}
