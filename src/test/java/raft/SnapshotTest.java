package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import communication.Pipe;
import message.DictMsg;
import message.Message;
import message.Snapshot;

class SnapshotTest {

    private static final String LEVEL = "Shard";
    private final String[] ids = {"snapTestA", "snapTestB"};

    private Map<String, Node> config() {
        Map<String, Node> nodes = new HashMap<>();
        nodes.put("snapTestA", new Node("snapTestA", "localhost", 1));
        nodes.put("snapTestB", new Node("snapTestB", "localhost", 2));
        return nodes;
    }

    private RaftState newState(String id, Pipe out) {
        return new RaftState(id, out, LEVEL, config(), new Pipe(), false);
    }

    private static Message drainLast(Pipe pipe) throws Exception {
        Message last = null;
        while (pipe.size() > 0)
            last = pipe.take();
        return last;
    }

    @AfterEach
    void cleanup() throws Exception {
        for (String id : ids) {
            RaftState s = newState(id, new Pipe());
            Files.deleteIfExists(s.log.getSnapshotPath());
        }
    }

    @Test
    void compactsCommittedLogAndRestoresOnStartup() throws Exception {
        int n = RaftLog.SNAPSHOT_THRESHOLD + 5;
        Pipe out = new Pipe();
        RaftState state = newState("snapTestA", out);
        state.term = 3;
        for (int i = 0; i < n; i++)
            state.log.appendEntry(new DictMsg("Put", "k" + (i % 10), "v" + i, 0), 3);
        state.log.appendEntry(new DictMsg("Delete", "k0", null, 0), 3);
        state.log.commitEntries(n - 1); // leave the Delete uncommitted

        assertEquals(n - 1, state.log.getSnapshotLastIndex());
        assertEquals(n - 1, state.log.getCommitIdx());
        assertEquals(n, state.log.getLastIdx());
        assertNull(state.log.get(0));
        assertEquals(3, state.log.getTerm(n - 1));
        assertTrue(Files.exists(state.log.getSnapshotPath()));

        // a fresh node with the same identity restores from disk
        Pipe restoredOut = new Pipe();
        RaftState restored = newState("snapTestA", restoredOut);
        assertEquals(n - 1, restored.log.getCommitIdx());
        assertEquals(n - 1, restored.log.getLastIdx());
        assertEquals(3, restored.log.getLastTerm());
        assertEquals(3, restored.term);

        Message pushed = restoredOut.take(100);
        assertNotNull(pushed);
        assertEquals("Snapshot", pushed.type);
        Snapshot snap = (Snapshot) pushed;
        assertEquals(10, snap.kvStore.size());
        assertEquals("v" + (n - 1), snap.kvStore.get("k" + ((n - 1) % 10)));

        // new entries continue from the snapshot index
        restored.log.appendEntry(new DictMsg("Put", "x", "y", 0), 4);
        assertEquals(n, restored.log.getLastIdx());
        assertEquals(n, restored.log.get(n).index);
    }

    @Test
    void followerInstallsLeaderSnapshot() throws Exception {
        int n = RaftLog.SNAPSHOT_THRESHOLD;
        RaftState leader = newState("snapTestA", new Pipe());
        for (int i = 0; i < n; i++)
            leader.log.appendEntry(new DictMsg("Put", "k" + i, "v" + i, 0), 1);
        leader.log.commitEntries(n - 1);
        Snapshot snap = leader.log.getLatestSnapshot();
        assertNotNull(snap);

        Pipe followerOut = new Pipe();
        RaftState follower = newState("snapTestB", followerOut);
        follower.log.appendEntry(new DictMsg("Put", "stale", "x", 0), 1);
        assertTrue(follower.log.installSnapshot(snap));
        assertEquals(n - 1, follower.log.getCommitIdx());
        assertEquals(n - 1, follower.log.getLastIdx());

        Snapshot pushed = (Snapshot) drainLast(followerOut);
        assertEquals(n, pushed.kvStore.size());
        assertTrue(Files.exists(follower.log.getSnapshotPath()));

        // installing the same snapshot again is a no-op
        assertFalse(follower.log.installSnapshot(snap));
    }
}
