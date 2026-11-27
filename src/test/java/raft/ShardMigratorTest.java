package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import message.MigrateData;
import testutil.Capture;
import testutil.Nodes;

class ShardMigratorTest {

    private static final String LOG = "logs/ShardRaft.log";
    private Capture capture;
    private Set<String> pending;

    @BeforeEach
    void setUp() throws Exception {
        capture = new Capture();
        pending = ConcurrentHashMap.newKeySet();
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
    }

    @Test
    void ordersEstablishedNodesFirstAndSkipsSelf() {
        Map<String, Node> shard = Nodes.map(
            new Node("self", "h", 1), new Node("moved", "h", 2), new Node("old1", "h", 3));
        Map<String, Node> moved = Nodes.map(new Node("self", "h", 1), new Node("moved", "h", 2));

        List<Node> order = ShardMigrator.orderTargets(shard, moved, "self");
        assertEquals(List.of("old1", "moved"), order.stream().map(n -> n.id).toList());
    }

    @Test
    void retriesUntilAcknowledged() throws Exception {
        MigrateData m = new MigrateData("m1", "shard1", Map.of("k", "v"), capture.node("self"));
        ShardMigrator.start(m, List.of(capture.node("t1"), capture.node("t2")), pending, LOG);
        assertTrue(pending.contains("m1"));

        MigrateData first = capture.next(MigrateData.class, 2000);
        assertEquals("m1", first.id);
        assertEquals(Map.of("k", "v"), first.entries);
        assertNotNull(capture.next(MigrateData.class, 3000)); // resent without an ack

        pending.remove("m1"); // what RaftNode does when the MigrateAck arrives
        capture.next(MigrateData.class, 2500); // at most one send already in flight
        assertNull(capture.next(MigrateData.class, 2500));
    }

    @Test
    void givesUpImmediatelyWithNoTargets() throws Exception {
        ShardMigrator.start(new MigrateData("m2", "shard9", Map.of("k", "v"), null), List.of(), pending, LOG);
        long deadline = System.currentTimeMillis() + 2000;
        while (pending.contains("m2") && System.currentTimeMillis() < deadline)
            Thread.sleep(20);
        assertFalse(pending.contains("m2"));
    }
}
