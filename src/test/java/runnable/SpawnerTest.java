package runnable;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import communication.HandOff;
import communication.Listener;
import message.Ack;
import message.Message;
import message.MessageDeserializer;
import message.NodeMsg;
import message.Snapshot;
import testutil.Capture;
import testutil.Nodes;

class SpawnerTest {

    private final Path older = Paths.get(Spawner.SNAPSHOT_DIR, "spawnTestOld-Cluster.json");
    private final Path newer = Paths.get(Spawner.SNAPSHOT_DIR, "spawnTestNew-Cluster.json");

    @AfterEach
    void cleanup() throws Exception {
        Files.deleteIfExists(older);
        Files.deleteIfExists(newer);
    }

    private void write(Path path, int index, Snapshot.MembershipOp... ops) throws Exception {
        Snapshot snap = new Snapshot(index, 1, 1, null, null, null, 0);
        snap.membershipOps = List.of(ops);
        Files.createDirectories(path.getParent());
        Files.writeString(path, new Gson().toJson(snap));
    }

    @Test
    void replaysNewestClusterSnapshotOverSeeds() throws Exception {
        Spawner.gson = new Gson();
        List<Node> seeds = List.of(
            new Node("S0", "localhost", 4567),
            new Node("S1", "localhost", 4568));

        write(older, 10, new Snapshot.MembershipOp("Add", new Node("stale", "localhost", 9999)));
        write(newer, 250,
            new Snapshot.MembershipOp("Add", new Node("N1", "localhost", 4570)),
            new Snapshot.MembershipOp("Add", new Node("N2", "localhost", 4571)),
            new Snapshot.MembershipOp("Remove", new Node("S1", null, -1)));

        Map<String, Node> members = Spawner.currentMembership(seeds);
        assertEquals(List.of("S0", "N1", "N2"), List.copyOf(members.keySet()));
        assertEquals(4571, members.get("N2").port);
    }

    @Test
    void createCommandForwardsRaftTuningProperties() throws Exception {
        System.setProperty("raft.heartbeatMs", "50");
        try {
            assertEquals("java -Draft.heartbeatMs=50 -jar target/DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar N1 4570 true",
                Spawner.createCommand("N1", "4570", true));
        } finally {
            System.clearProperty("raft.heartbeatMs");
        }
    }

    @Test
    void killAndReviveOnlyApplyToKnownNodesInTheRightState() throws Exception {
        int port = Nodes.closedPort();
        Spawner.gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageDeserializer()).create();
        Spawner.nodes = new HashMap<>();
        Spawner.members = new HashMap<>();
        Spawner.seedIds = new HashMap<>();
        Spawner.seedIds.put("S0", true);
        Spawner.listener = new Listener();
        Spawner.listener.createSocket(port);

        // stands in for a node's process thread: runs until it is interrupted
        Thread running = new Thread(() -> {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException ignored) {
            }
        });
        running.start();
        Node n1 = new Node("N1", "localhost", 4570);
        Spawner.nodes.put("N1", running);
        Spawner.members.put("N1", n1);

        try (Capture client = new Capture()) {
            Node c = client.node("client");

            assertFalse(request(new NodeMsg("Kill", new Node("N9", null, -1), 0, c), client, port).success);
            assertFalse(request(new NodeMsg("Revive", new Node("N9", null, -1), 0, c), client, port).success);

            Ack notDown = request(new NodeMsg("Revive", new Node("N1", null, -1), 0, c), client, port);
            assertFalse(notDown.success);
            assertTrue(notDown.message.contains("already running"));

            // Kill stops the process but remembers the node and its port
            Ack killed = request(new NodeMsg("Kill", new Node("N1", null, -1), 0, c), client, port);
            assertTrue(killed.success);
            assertEquals(4570, killed.node.port);
            assertFalse(running.isAlive(), "kill should wait for the process to stop");
            assertFalse(Spawner.nodes.containsKey("N1"));
            assertEquals(n1, Spawner.members.get("N1"));

            Ack again = request(new NodeMsg("Kill", new Node("N1", null, -1), 0, c), client, port);
            assertFalse(again.success);
            assertTrue(again.message.contains("already killed"));

            // a killed member keeps its id: it must be revived, not added again
            Ack add = request(new NodeMsg("Add", new Node("N1", null, -1), 0, c), client, port);
            assertFalse(add.success);
            assertTrue(add.message.contains("Revive"));

            // a killed member can still be removed from the cluster
            assertTrue(request(new NodeMsg("Remove", new Node("N1", null, -1), 0, c), client, port).success);
            assertFalse(Spawner.members.containsKey("N1"));
            assertFalse(request(new NodeMsg("Revive", new Node("N1", null, -1), 0, c), client, port).success);

            assertFalse(request(new NodeMsg("Restart", new Node("N1", null, -1), 0, c), client, port).success);
        } finally {
            Spawner.listener.closeSocket();
        }
    }

    @Test
    void newNodesSkipPortsInUseOrHeldByKilledMembers() throws Exception {
        Spawner.members = new HashMap<>();
        try (java.net.ServerSocket taken = new java.net.ServerSocket(0)) {
            assertFalse(Spawner.portFree(taken.getLocalPort()));
        }
        int free = Nodes.closedPort();
        assertTrue(Spawner.portFree(free));
        Spawner.members.put("dead", new Node("dead", "localhost", free));
        assertFalse(Spawner.portFree(free));
    }

    @Test
    void onlySeedsStartAsVoters() {
        Spawner.seedIds = new HashMap<>();
        Spawner.seedIds.put("SEED0", true);
        assertFalse(Spawner.isLearner("SEED0"));
        assertTrue(Spawner.isLearner("N1"));
    }

    @Test
    void createCommandLaunchesTheJarWithNodeArguments() throws Exception {
        assertEquals("java -jar target/DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar N1 4570 true",
            Spawner.createCommand("N1", "4570", true));
    }

    /** Sends a node-management request to the spawner and returns its acknowledgement. */
    private static Ack request(NodeMsg msg, Capture client, int port) throws Exception {
        HandOff.sendToNode(new Node("spawner", "localhost", port), new Gson().toJson(msg), "logs/test.log");
        Spawner.handleConnection();
        return client.next(Ack.class, 2000);
    }

    @Test
    void handlesRemoveAndRejectsBadRequestsWithoutLaunchingProcesses() throws Exception {
        int port = Nodes.closedPort();
        Spawner.gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageDeserializer()).create();
        Spawner.nodes = new HashMap<>();
        Spawner.listener = new Listener();
        Spawner.listener.createSocket(port);
        Thread running = new Thread(() -> {});
        Spawner.nodes.put("N1", running);

        try (Capture client = new Capture()) {
            Node c = client.node("client");

            Ack dup = request(new NodeMsg("Add", new Node("N1", null, -1), 0, c), client, port);
            assertFalse(dup.success);
            assertTrue(dup.message.contains("already exists"));

            Ack missing = request(new NodeMsg("Remove", new Node("N9", null, -1), 0, c), client, port);
            assertFalse(missing.success);

            NodeMsg noNode = new NodeMsg("Remove", new Node("x", null, -1), 0, c);
            noNode.node = null;
            assertFalse(request(noNode, client, port).success);

            Ack removed = request(new NodeMsg("Remove", new Node("N1", null, -1), 0, c), client, port);
            assertTrue(removed.success);
            assertEquals("N1", removed.node.id);
            assertFalse(Spawner.nodes.containsKey("N1"));
        } finally {
            Spawner.listener.closeSocket();
        }
    }
}
