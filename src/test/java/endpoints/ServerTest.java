package endpoints;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import cluster.Node;
import communication.HandOff;
import communication.Pipe;
import message.AppendEntries;
import message.Config;
import message.DictMsg;
import message.InstallSnapshot;
import message.Message;
import message.MigrateAck;
import message.MigrateData;
import message.NodeMsg;
import message.RequestVote;
import message.Snapshot;
import testutil.Capture;
import testutil.Nodes;

class ServerTest {

    private static Node self;

    @BeforeAll
    static void start() throws Exception {
        int port = Nodes.closedPort();
        Server.init(new String[] {"serverTest", String.valueOf(port), "true"});
        Server.listener.createSocket(port);
        self = new Node("serverTest", "localhost", port);
    }

    @AfterAll
    static void stop() throws Exception {
        Server.listener.closeSocket();
    }

    @Test
    void initReadsArgumentsAndSeedNodes() {
        assertEquals("serverTest", Server.nodeId);
        assertTrue(Server.learner);
        assertFalse(Server.configData.isEmpty());
        assertFalse(Server.configData.containsKey("spawner"));
    }

    /** Sends a message to the server, handles one connection, and returns the pipe it landed in. */
    private static Message route(Message msg, Pipe expected) throws Exception {
        HandOff.sendToNode(self, Capture.GSON.toJson(msg), "logs/test.log");
        Server.handleConnection();
        Message received = expected.take(1000);
        assertNotNull(received, msg.type + " did not reach the expected pipe");
        return received;
    }

    @Test
    void routesEachMessageToItsComponent() throws Exception {
        Node client = new Node("client", "localhost", 1);
        assertTrue(route(new DictMsg("Put", "k", "v", 0, client), Server.shardRaftIn) instanceof DictMsg);
        assertTrue(route(new MigrateData("m", "s", Map.of(), client), Server.shardRaftIn) instanceof MigrateData);
        assertTrue(route(new MigrateAck("m"), Server.shardRaftIn) instanceof MigrateAck);
        assertTrue(route(new Config(client), Server.clusterStateIn) instanceof Config);
        assertTrue(route(new NodeMsg("Add", client, 0, client), Server.clusterRaftIn) instanceof NodeMsg);
    }

    @Test
    void routesRaftTrafficByLevel() throws Exception {
        route(new AppendEntries("Shard", 1, "L", -1, 0, -1, null, 0), Server.shardRaftIn);
        route(new RequestVote("Cluster", 1, "C", -1, 0, 0), Server.clusterRaftIn);
        Snapshot snap = new Snapshot(1, 1, 1, null, null, null, 0);
        route(new InstallSnapshot("Cluster", 1, "L", snap, 0), Server.clusterRaftIn);
        assertEquals(0, Server.shardRaftIn.size());
    }
}
