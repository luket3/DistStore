package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import communication.Pipe;
import message.AppendEntries;
import message.AppendEntriesReply;
import message.DictMsg;
import message.InstallSnapshot;
import message.RequestVote;
import message.RequestVoteReply;
import message.Snapshot;
import testutil.Capture;
import testutil.KvFake;
import testutil.Nodes;

class FollowerTest {

    private static final String ID = "followerTestF";
    private Capture capture;
    private KvFake kv;
    private RaftState state;
    private Follower follower;
    private Map<String, Node> nodes;

    @BeforeEach
    void setUp() throws Exception {
        Nodes.deleteSnapshots(ID);
        capture = new Capture();
        kv = new KvFake();
        nodes = Nodes.map(Nodes.unreachable(ID), capture.node("L"), capture.node("C"));
        state = new RaftState(ID, kv, "Shard", nodes, new Pipe(), false);
        follower = new Follower(state);
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
        Nodes.deleteSnapshots(ID);
    }

    private static ArrayList<LogEntry> entries(int startIndex, int term, String... keys) {
        ArrayList<LogEntry> list = new ArrayList<>();
        for (int i = 0; i < keys.length; i++)
            list.add(new LogEntry(new DictMsg("Put", keys[i], "v" + keys[i], 0), term, startIndex + i));
        return list;
    }

    private AppendEntries ae(int term, int prevIdx, int prevTerm, int commit, ArrayList<LogEntry> entries) {
        return new AppendEntries("Shard", term, "L", prevIdx, prevTerm, commit, entries, 0);
    }

    @Test
    void grantsOneVotePerTerm() throws Exception {
        assertTrue(follower.requestVote(new RequestVote("Shard", 1, "C", -1, 0, 0)));
        assertEquals("C", state.votedFor);
        assertTrue(capture.next(RequestVoteReply.class, 2000).voteGranted);

        assertFalse(follower.requestVote(new RequestVote("Shard", 1, "L", -1, 0, 0)));
        assertFalse(capture.next(RequestVoteReply.class, 2000).voteGranted);

        // a new term resets the vote
        assertTrue(follower.requestVote(new RequestVote("Shard", 2, "L", -1, 0, 0)));
        assertEquals(2, state.term);
    }

    @Test
    void rejectsStaleTermsAndOutOfDateLogs() throws Exception {
        state.term = 3;
        assertFalse(follower.requestVote(new RequestVote("Shard", 2, "C", 10, 2, 0)));
        assertNull(capture.next(300)); // stale requests get no reply

        state.log.appendEntry(new DictMsg("Put", "a", "1", 0), 3);
        assertFalse(follower.requestVote(new RequestVote("Shard", 3, "C", 5, 2, 0)));  // older last term
        assertFalse(follower.requestVote(new RequestVote("Shard", 4, "C", -1, 3, 0))); // shorter log
    }

    @Test
    void appendsEntriesAndCommitsUpToLeaderCommit() throws Exception {
        assertTrue(follower.appendEntries(ae(1, -1, 0, 0, entries(0, 1, "a", "b"))));
        assertEquals(1, state.log.getLastIdx());
        assertEquals(0, state.log.getCommitIdx());
        assertEquals(Map.of("a", "va"), kv.store);
        assertEquals("L", state.leader.id);

        AppendEntriesReply reply = capture.next(AppendEntriesReply.class, 2000);
        assertTrue(reply.success);
        assertEquals(1, reply.matchIndex);

        // heartbeat advancing the commit point
        follower.appendEntries(ae(1, 1, 1, 1, null));
        assertEquals(1, state.log.getCommitIdx());
        assertEquals(2, kv.store.size());
    }

    @Test
    void rejectsEntriesThatDoNotFollowItsLog() throws Exception {
        follower.appendEntries(ae(1, 5, 1, -1, entries(6, 1, "x")));
        AppendEntriesReply reply = capture.next(AppendEntriesReply.class, 2000);
        assertFalse(reply.success);
        assertEquals(-1, state.log.getLastIdx());

        follower.appendEntries(ae(1, -1, 0, -1, entries(0, 1, "a")));
        follower.appendEntries(ae(1, 0, 7, -1, entries(1, 1, "b"))); // wrong prevLogTerm
        capture.drain(200);
        assertEquals(0, state.log.getLastIdx());
    }

    @Test
    void conflictingEntriesAreReplaced() {
        follower.appendEntries(ae(1, -1, 0, -1, entries(0, 1, "a", "b", "c")));
        follower.appendEntries(ae(2, 0, 1, -1, entries(1, 2, "x")));
        assertEquals(1, state.log.getLastIdx());
        assertEquals(2, state.log.get(1).term);
        assertEquals("x", ((DictMsg) state.log.get(1).msg).key);
    }

    @Test
    void staleLeaderIsIgnored() throws Exception {
        state.term = 5;
        assertFalse(follower.appendEntries(ae(4, -1, 0, -1, entries(0, 4, "a"))));
        assertEquals(-1, state.log.getLastIdx());
        assertNull(capture.next(300));
    }

    @Test
    void installsSnapshotFromLeader() throws Exception {
        Snapshot snap = new Snapshot(9, 2, 2, null, nodes, nodes, 0);
        snap.kvStore.put("k", "v");
        assertTrue(follower.installSnapshot(new InstallSnapshot("Shard", 2, "L", snap, 0)));

        assertEquals(9, state.log.getCommitIdx());
        assertEquals(2, state.term);
        assertEquals(Map.of("k", "v"), kv.store);
        AppendEntriesReply reply = capture.next(AppendEntriesReply.class, 2000);
        assertTrue(reply.success);
        assertEquals(9, reply.matchIndex);

        // entries after the snapshot are accepted
        follower.appendEntries(ae(2, 9, 2, 10, entries(10, 2, "z")));
        assertEquals(10, state.log.getCommitIdx());
        assertEquals("vz", kv.store.get("z"));
    }

    @Test
    void forwardsToKnownLeaderOnly() throws Exception {
        follower.handToLeader(new DictMsg("Get", "k", null, 0));
        assertNull(capture.next(300));

        state.leader = capture.node("L");
        follower.handToLeader(new DictMsg("Get", "k", null, 0));
        assertEquals("k", capture.next(DictMsg.class, 2000).key);
    }
}
