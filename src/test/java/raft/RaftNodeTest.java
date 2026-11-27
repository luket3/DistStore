package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import communication.Pipe;
import message.AppendEntries;
import message.AppendEntriesReply;
import message.DictMsg;
import message.MigrateAck;
import message.RequestVote;
import message.RequestVoteReply;
import message.Response;
import testutil.Capture;
import testutil.KvFake;
import testutil.Nodes;

class RaftNodeTest {

    private Capture capture;

    @BeforeEach
    void setUp() throws Exception {
        capture = new Capture();
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
        Nodes.deleteSnapshots("A");
    }

    private RaftNode node(boolean learner) {
        return new RaftNode("A", new KvFake(), "Shard",
            Nodes.map(Nodes.unreachable("A"), capture.node("B"), capture.node("C")), new Pipe(), learner);
    }

    private static RaftState stateOf(RaftNode node) throws Exception {
        Field f = RaftNode.class.getDeclaredField("raftState");
        f.setAccessible(true);
        return (RaftState) f.get(node);
    }

    @Test
    void reportsRole() {
        assertEquals("follower", node(false).getRole());
        assertFalse(node(false).learner());
        assertTrue(node(true).learner());
    }

    @Test
    void discardsRaftTrafficFromOutsideTheConfiguration() throws Exception {
        RaftNode n = node(false);
        ArrayList<LogEntry> entries = new ArrayList<>(List.of(new LogEntry(new DictMsg("Put", "k", "v", 0), 1, 0)));
        n.handleMessage(new AppendEntries("Shard", 1, "stranger", -1, 0, -1, entries, 0));
        assertEquals(-1, stateOf(n).log.getLastIdx());
        assertEquals(0, stateOf(n).term);
    }

    @Test
    void acceptsAnUnknownLeaderThatSendsItsAddress() throws Exception {
        // e.g. a node restarted with an outdated membership while a newer node leads
        RaftNode n = node(false);
        ArrayList<LogEntry> entries = new ArrayList<>(List.of(new LogEntry(new DictMsg("Put", "k", "v", 0), 1, 0)));
        AppendEntries ae = new AppendEntries("Shard", 1, "N3", -1, 0, -1, entries, 0);
        ae.sender = capture.node("N3");
        n.handleMessage(ae);

        assertEquals(0, stateOf(n).log.getLastIdx(), "entry from the new leader should be appended");
        assertTrue(stateOf(n).activeNodes.containsKey("N3"));
        AppendEntriesReply reply = capture.next(AppendEntriesReply.class, 2000);
        assertNotNull(reply, "the node should reply to the new leader");
        assertTrue(reply.success);
    }

    @Test
    void ignoresAnUnknownLeaderFromAnOldTerm() throws Exception {
        RaftNode n = node(false);
        stateOf(n).term = 5;
        AppendEntries ae = new AppendEntries("Shard", 4, "N3", -1, 0, -1, new ArrayList<>(), 0);
        ae.sender = capture.node("N3");
        n.handleMessage(ae);
        assertFalse(stateOf(n).activeNodes.containsKey("N3"));
    }

    @Test
    void stillDiscardsVoteRequestsFromUnknownNodes() throws Exception {
        RaftNode n = node(false);
        n.handleMessage(new RequestVote("Shard", 1, "stranger", -1, 0, 0));
        assertEquals(0, stateOf(n).term);
        assertNull(capture.next(300));
    }

    @Test
    void confirmsAnUpdateItAlreadyHasStraightAway() throws Exception {
        // a restarted node replays committed membership changes its snapshot already covers
        RaftNode n = node(false);
        RaftState s = stateOf(n);
        s.version = 2;
        n.handleMessage(new message.Update(s.activeNodes, 1, false));
        message.Message ack = s.callbackPipe.take(1000);
        assertNotNull(ack, "the coordinator waiting on this update must be released");
        assertEquals(2, ack.version);
        assertEquals(2, s.version);
    }

    @Test
    void votesForKnownCandidates() throws Exception {
        node(false).handleMessage(new RequestVote("Shard", 1, "B", -1, 0, 0));
        assertTrue(capture.next(RequestVoteReply.class, 2000).voteGranted);
    }

    @Test
    void learnersDoNotVote() throws Exception {
        node(true).handleMessage(new RequestVote("Shard", 1, "B", -1, 0, 0));
        assertNull(capture.next(300));
    }

    @Test
    void leaderAppendsClientCommands() throws Exception {
        RaftNode n = node(false);
        RaftState s = stateOf(n);
        s.type = "leader";
        s.term = 1;
        s.initializeLeaderState();

        n.handleMessage(new DictMsg("Put", "k", "v", 0));

        assertEquals(0, s.log.getLastIdx());
        assertNotNull(capture.next(AppendEntries.class, 2000));
    }

    @Test
    void followerForwardsClientCommandsToLeader() throws Exception {
        RaftNode n = node(false);
        stateOf(n).leader = capture.node("B");
        n.handleMessage(new DictMsg("Get", "k", null, 0));
        assertEquals("k", capture.next(DictMsg.class, 2000).key);
    }

    @Test
    void rejectsCommandsFromAnOldConfiguration() throws Exception {
        RaftNode n = node(false);
        stateOf(n).version = 2;
        n.handleMessage(new DictMsg("Put", "k", "v", 1, capture.node("client")));
        assertEquals("Invalid config", capture.next(Response.class, 2000).msg);
        assertEquals(-1, stateOf(n).log.getLastIdx());
    }

    @Test
    void migrationAckStopsResending() throws Exception {
        RaftNode n = node(false);
        stateOf(n).pendingMigrations.add("m1");
        n.handleMessage(new MigrateAck("m1"));
        assertTrue(stateOf(n).pendingMigrations.isEmpty());
    }
}
