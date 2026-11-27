package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import communication.Pipe;
import message.Ack;
import message.DictMsg;
import message.RaftConfig;
import testutil.KvFake;
import testutil.Nodes;

class RaftLogTest {

    private static final String ID = "logTestA";
    private KvFake kv;
    private Pipe callback;
    private RaftState state;
    private RaftLog log;

    @BeforeEach
    void setUp() {
        Nodes.deleteSnapshots(ID);
        kv = new KvFake();
        callback = new Pipe();
        state = new RaftState(ID, kv, "Shard", Nodes.map(Nodes.unreachable(ID), Nodes.unreachable("logTestB")), callback, false);
        log = state.log;
    }

    @AfterEach
    void tearDown() {
        Nodes.deleteSnapshots(ID);
    }

    private static DictMsg put(String k) {
        return new DictMsg("Put", k, "v" + k, 0);
    }

    @Test
    void emptyLog() {
        assertEquals(-1, log.getLastIdx());
        assertEquals(-1, log.getCommitIdx());
        assertEquals(0, log.getLastTerm());
        assertEquals(0, log.getSize());
        assertEquals(0, log.getTerm(-1));
        assertNull(log.get(0));
        assertNull(log.get(0, 0));
        assertNull(log.getLastCommittedCommand());
    }

    @Test
    void appendAssignsSequentialIndexes() {
        log.appendEntry(put("a"), 1);
        log.appendEntry(put("b"), 1);
        log.appendEntry(put("c"), 2);
        assertEquals(2, log.getLastIdx());
        assertEquals(2, log.getLastTerm());
        assertEquals(3, log.getSize());
        for (int i = 0; i < 3; i++)
            assertEquals(i, log.get(i).index);
        assertEquals(1, log.getTerm(1));
        assertEquals(-1, log.getTerm(5));
    }

    @Test
    void commitAppliesEntriesUpToIndexOnce() {
        log.appendEntry(put("a"), 1);
        log.appendEntry(put("b"), 1);
        log.appendEntry(put("c"), 1);

        log.commitEntries(1);
        assertEquals(1, log.getCommitIdx());
        assertEquals(Map.of("a", "va", "b", "vb"), kv.store);
        assertEquals("b", ((DictMsg) log.getLastCommittedCommand()).key);

        log.commitEntries(1);
        assertEquals(2, kv.applied.size());

        log.commitEntries(10);
        assertEquals(2, log.getCommitIdx());
        assertEquals(3, kv.applied.size());
    }

    @Test
    void followersDropTheClientBeforeApplying() {
        Node client = new Node("client", "localhost", 1);
        log.appendEntry(new DictMsg("Put", "a", "1", 0, client), 1);
        log.commitEntries(0);
        assertNull(((DictMsg) kv.applied.get(0)).client);

        state.type = "leader";
        log.appendEntry(new DictMsg("Put", "b", "2", 0, client), 1);
        log.commitEntries(1);
        assertNotNull(((DictMsg) kv.applied.get(1)).client);
    }

    @Test
    void rangeReadsSpanCommittedAndUncommitted() {
        for (String k : new String[] {"a", "b", "c", "d"})
            log.appendEntry(put(k), 1);
        log.commitEntries(1);

        assertEquals(2, log.get(0, 1).size());           // committed only
        assertEquals(2, log.get(2, 3).size());           // uncommitted only
        assertEquals(4, log.get(0, 3).size());           // spanning
        assertEquals(1, log.get(1, 2).get(0).index);
        assertNull(log.get(2, 1));
        assertNull(log.get(0, 4));
        assertNull(log.get(-1, 1));
    }

    @Test
    void clearToOnlyRemovesUncommittedEntries() {
        for (String k : new String[] {"a", "b", "c", "d"})
            log.appendEntry(put(k), 1);
        log.commitEntries(0);

        log.clearTo(10);                 // beyond the end: no-op
        assertEquals(3, log.getLastIdx());
        log.clearTo(2);
        assertEquals(2, log.getLastIdx());
        log.clearTo(0);                  // at the commit point: drop all uncommitted
        assertEquals(0, log.getLastIdx());
        log.clearTo(-1);
        assertEquals(0, log.getLastIdx()); // committed entry survives
        assertEquals(0, log.getCommitIdx());
    }

    @Test
    void committedConfigIsAppliedAndAcknowledged() throws Exception {
        log.appendEntry(new RaftConfig(state.voters, state.voters, true, 1), 1);
        assertTrue(log.uncommitedJointConfig);
        log.commitEntries(0);
        assertFalse(log.uncommitedJointConfig);
        assertTrue(state.jointConfig);
        assertTrue(kv.applied.isEmpty());        // configs never reach the state machine

        log.appendEntry(new RaftConfig(state.voters, null, false, 1), 1);
        log.commitEntries(1);
        assertFalse(state.jointConfig);
        assertEquals(1, state.version);
        assertTrue(callback.take(100) instanceof Ack);
    }

    @Test
    void wipeResetsLogAndDeletesSnapshot() {
        for (int i = 0; i < RaftLog.SNAPSHOT_THRESHOLD; i++)
            log.appendEntry(put("k" + i), 1);
        log.commitEntries(RaftLog.SNAPSHOT_THRESHOLD - 1);
        assertTrue(Files.exists(log.getSnapshotPath()));

        log.wipe();
        assertEquals(-1, log.getLastIdx());
        assertEquals(-1, log.getSnapshotLastIndex());
        assertNull(log.getLatestSnapshot());
        assertFalse(Files.exists(log.getSnapshotPath()));
    }
}
