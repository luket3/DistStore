package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import communication.Pipe;
import message.AppendEntries;
import message.AppendEntriesReply;
import message.DictMsg;
import message.InstallSnapshot;
import message.Message;
import testutil.Capture;
import testutil.KvFake;
import testutil.Nodes;

class LeaderTest {

    private static final String ID = "leaderTestA";
    private Capture capture;
    private KvFake kv;
    private RaftState state;
    private Leader leader;

    @BeforeEach
    void setUp() throws Exception {
        Nodes.deleteSnapshots(ID);
        capture = new Capture();
        kv = new KvFake();
        state = new RaftState(ID, kv, "Shard",
            Nodes.map(Nodes.unreachable(ID), capture.node("B"), capture.node("C")), new Pipe(), false);
        state.term = 1;
        state.type = "leader";
        leader = new Leader(state);
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
        Nodes.deleteSnapshots(ID);
    }

    private AppendEntriesReply reply(String from, boolean ok, int match) {
        return new AppendEntriesReply("Shard", 1, from, ok, match, 0);
    }

    @Test
    void appendingAnEntryReplicatesItToEveryPeer() throws Exception {
        state.initializeLeaderState();
        leader.appendLogEntry(new DictMsg("Put", "k", "v", 0));

        assertEquals(0, state.log.getLastIdx());
        assertEquals(0, state.matchIndex.get(ID));
        List<Message> sent = capture.drain(300);
        assertEquals(2, sent.size());
        for (Message m : sent) {
            AppendEntries ae = (AppendEntries) m;
            assertEquals(-1, ae.prevLogIndex);
            assertEquals(1, ae.entries.size());
            assertEquals(-1, ae.leaderCommit);
        }
    }

    @Test
    void commitsOnceAMajorityHasTheEntry() throws Exception {
        state.initializeLeaderState();
        leader.appendLogEntry(new DictMsg("Put", "k", "v", 0));
        capture.drain(200);

        leader.appendEntries(reply("B", true, 0));

        assertEquals(0, state.log.getCommitIdx());
        assertEquals("v", kv.store.get("k"));
        assertEquals(1, state.nextIndex.get("B"));
        // followers are told about the new commit point
        AppendEntries ae = capture.next(AppendEntries.class, 2000);
        assertEquals(0, ae.leaderCommit);
    }

    @Test
    void neverCommitsEntriesFromOlderTermsByCounting() {
        state.log.appendEntry(new DictMsg("Put", "k", "v", 0), 0); // entry from term 0
        state.initializeLeaderState();
        state.matchIndex.put(ID, 0);
        leader.appendEntries(reply("B", true, 0));
        assertEquals(-1, state.log.getCommitIdx());
    }

    @Test
    void failedReplyBacksOffAndRetries() throws Exception {
        for (String k : new String[] {"a", "b", "c"})
            state.log.appendEntry(new DictMsg("Put", k, "v", 0), 1);
        state.log.commitEntries(2);
        state.initializeLeaderState();

        leader.appendEntries(reply("B", false, 0));

        assertEquals(2, state.nextIndex.get("B"));
        AppendEntries retry = capture.next(AppendEntries.class, 2000);
        assertEquals(1, retry.prevLogIndex);
        assertEquals(1, retry.prevLogTerm);
        assertEquals(1, retry.entries.size());
    }

    @Test
    void higherTermReplyStepsDown() {
        state.initializeLeaderState();
        leader.appendEntries(new AppendEntriesReply("Shard", 4, "B", false, -1, 0));
        assertEquals("follower", state.type);
        assertEquals(4, state.term);
    }

    @Test
    void sendsSnapshotToFollowersBehindIt() throws Exception {
        int n = RaftLog.SNAPSHOT_THRESHOLD;
        for (int i = 0; i < n; i++)
            state.log.appendEntry(new DictMsg("Put", "k" + i, "v", 0), 1);
        state.log.commitEntries(n - 1);
        state.initializeLeaderState();
        state.nextIndex.put("B", 0);

        leader.sendAppendEntries(capture.node("B"));

        InstallSnapshot is = capture.next(InstallSnapshot.class, 2000);
        assertNotNull(is);
        assertEquals(n - 1, is.snapshot.lastIncludedIndex);
        assertEquals(n, is.snapshot.kvStore.size());
    }

    @Test
    void heartbeatWithNothingNewCarriesNoEntries() throws Exception {
        state.initializeLeaderState();
        leader.broadcastAppendEntries();
        List<Message> sent = capture.drain(300);
        assertEquals(2, sent.size());
        assertNull(((AppendEntries) sent.get(0)).entries);
    }
}
