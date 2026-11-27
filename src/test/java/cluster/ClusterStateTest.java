package cluster;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import communication.Pipe;
import message.Ack;
import message.Config;
import message.Message;
import message.NodeMsg;
import message.Response;
import message.Snapshot;
import message.Update;
import testutil.Capture;

class ClusterStateTest {

    private Pipe in, shardRaftIn, clusterRaftIn, shardAck, clusterAck;
    private ClusterState cs;
    private Capture capture;

    private static Map<String, Node> seeds() {
        Map<String, Node> s = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++)
            s.put("S" + i, new Node("S" + i, "localhost", 4000 + i));
        return s;
    }

    @BeforeEach
    void setUp() throws Exception {
        in = new Pipe();
        shardRaftIn = new Pipe();
        clusterRaftIn = new Pipe();
        shardAck = new Pipe();
        clusterAck = new Pipe();
        cs = new ClusterState(in, seeds(), "S0", shardRaftIn, clusterRaftIn, shardAck, clusterAck);
        capture = new Capture();
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
    }

    @Test
    void applyingAnAddQueuesFollowUpWork() throws Exception {
        cs.apply(new NodeMsg("Add", new Node("N0", "localhost", 5000), 0));
        Message pending = in.take(100);
        assertNotNull(pending);
        assertEquals("PendingUpdate", pending.type);
        assertEquals(1, pending.version);
    }

    @Test
    void rejectedChangesQueueNothing() throws Exception {
        cs.apply(new NodeMsg("Add", "N0", 0));          // no address
        cs.apply(new NodeMsg("Rename", new Node("N0", "localhost", 1), 0));
        cs.apply(new Response("not a NodeMsg"));
        assertNull(in.take(100));

        Snapshot snap = new Snapshot(0, 0, 0, null, null, null, 0);
        cs.fillSnapshot(snap);
        assertTrue(snap.membershipOps.isEmpty());
    }

    @Test
    void coordinatorPublishesUpdatesWaitsForAcksAndRepliesToClient() throws Exception {
        cs.apply(new NodeMsg("Add", new Node("N0", "localhost", 5000), 0, capture.node("client")));
        // acks the Raft groups would send once the new config is applied
        // a stale confirmation from an earlier version is skipped
        clusterAck.put(Ack.forVersion(0));
        clusterAck.put(Ack.forVersion(1));
        shardAck.put(Ack.forVersion(1));

        cs.takeAndHandle();

        Update clusterUpdate = (Update) clusterRaftIn.take(100);
        assertEquals(1, clusterUpdate.version);
        assertTrue(clusterUpdate.nodes().containsKey("N0"));
        assertEquals(4, clusterUpdate.nodes().size());

        Update shardUpdate = (Update) shardRaftIn.take(100);
        assertFalse(shardUpdate.shardRemoved);
        assertEquals(4, shardUpdate.nodes().size());

        Response reply = capture.next(Response.class, 2000);
        assertEquals("Cluster successfully updated", reply.msg);
        assertEquals(0, clusterAck.size());
        assertEquals(0, shardAck.size());
    }

    @Test
    void answersConfigRequestsWithCurrentView() throws Exception {
        cs.apply(new NodeMsg("Add", new Node("N0", "localhost", 5000), 0));
        in.take(100); // discard the queued follow-up

        in.put(new Config(capture.node("client")));
        cs.takeAndHandle();

        Config reply = capture.next(Config.class, 2000);
        assertNotNull(reply);
        assertEquals(1, reply.version);
        assertEquals(4, reply.config.getAllNodes().size());
    }

    @Test
    void ignoresUnknownMessagesOnTheCoordinatorPipe() throws Exception {
        in.put(new Response("junk"));
        cs.takeAndHandle();
        assertEquals(0, clusterRaftIn.size());
        assertEquals(0, shardRaftIn.size());
    }

    @Test
    void snapshotRecordsMembershipChangesInOrder() throws Exception {
        cs.apply(new NodeMsg("Add", new Node("N0", "localhost", 5000), 0));
        cs.apply(new NodeMsg("Remove", "N0", 0));
        Snapshot snap = new Snapshot(0, 0, 0, null, null, null, 0);
        cs.fillSnapshot(snap);
        assertEquals(2, snap.membershipOps.size());
        assertEquals("Add", snap.membershipOps.get(0).action);
        assertEquals("Remove", snap.membershipOps.get(1).action);
    }
}
