package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import cluster.ClusterState;
import cluster.Node;
import communication.Pipe;
import message.DictMsg;
import message.Message;
import message.NodeMsg;
import message.Snapshot;

class SnapshotTest {

    private static final String LEVEL = "Shard";
    private final String[] ids = {"snapTestA", "snapTestB"};

    /** Minimal key-value state machine that records what Raft did to it. */
    static class FakeKv implements ReplicatedStateMachine {
        final Map<String, String> store = new HashMap<>();
        int applied = 0;
        Snapshot restoredFrom = null;

        @Override
        public void apply(Message msg) {
            DictMsg d = (DictMsg) msg;
            if (d.action.equals("Put")) store.put(d.key, d.value);
            else if (d.action.equals("Delete")) store.remove(d.key);
            applied++;
        }

        @Override
        public void fillSnapshot(Snapshot snapshot) {
            snapshot.kvStore = new HashMap<>(store);
        }

        @Override
        public void restore(Snapshot snapshot) {
            store.clear();
            store.putAll(snapshot.kvStore);
            restoredFrom = snapshot;
        }
    }

    private Map<String, Node> config() {
        Map<String, Node> nodes = new HashMap<>();
        nodes.put("snapTestA", new Node("snapTestA", "localhost", 1));
        nodes.put("snapTestB", new Node("snapTestB", "localhost", 2));
        return nodes;
    }

    private RaftState newState(String id, ReplicatedStateMachine sm) {
        return new RaftState(id, sm, LEVEL, config(), new Pipe(), false);
    }

    @AfterEach
    void cleanup() throws Exception {
        for (String id : ids) {
            RaftState s = newState(id, new FakeKv());
            Files.deleteIfExists(s.log.getSnapshotPath());
        }
    }

    @Test
    void compactsCommittedLogAndRestoresOnStartup() throws Exception {
        int n = RaftLog.SNAPSHOT_THRESHOLD + 5;
        FakeKv kv = new FakeKv();
        RaftState state = newState("snapTestA", kv);
        state.term = 3;
        for (int i = 0; i < n; i++)
            state.log.appendEntry(new DictMsg("Put", "k" + (i % 10), "v" + i, 0), 3);
        state.log.appendEntry(new DictMsg("Delete", "k0", null, 0), 3);
        state.log.commitEntries(n - 1); // leave the Delete uncommitted

        assertEquals(n, kv.applied);
        assertEquals(n - 1, state.log.getSnapshotLastIndex());
        assertEquals(n - 1, state.log.getCommitIdx());
        assertEquals(n, state.log.getLastIdx());
        assertNull(state.log.get(0));
        assertEquals(3, state.log.getTerm(n - 1));
        assertTrue(Files.exists(state.log.getSnapshotPath()));

        // a fresh node with the same identity restores from disk
        FakeKv restoredKv = new FakeKv();
        RaftState restored = newState("snapTestA", restoredKv);
        assertEquals(n - 1, restored.log.getCommitIdx());
        assertEquals(n - 1, restored.log.getLastIdx());
        assertEquals(3, restored.log.getLastTerm());
        assertEquals(3, restored.term);
        assertNotNull(restoredKv.restoredFrom);
        assertEquals(kv.store, restoredKv.store);

        // new entries continue from the snapshot index
        restored.log.appendEntry(new DictMsg("Put", "x", "y", 0), 4);
        assertEquals(n, restored.log.getLastIdx());
        assertEquals(n, restored.log.get(n).index);
    }

    @Test
    void followerInstallsLeaderSnapshot() throws Exception {
        int n = RaftLog.SNAPSHOT_THRESHOLD;
        FakeKv leaderKv = new FakeKv();
        RaftState leader = newState("snapTestA", leaderKv);
        for (int i = 0; i < n; i++)
            leader.log.appendEntry(new DictMsg("Put", "k" + i, "v" + i, 0), 1);
        leader.log.commitEntries(n - 1);
        Snapshot snap = leader.log.getLatestSnapshot();
        assertNotNull(snap);

        FakeKv followerKv = new FakeKv();
        RaftState follower = newState("snapTestB", followerKv);
        follower.log.appendEntry(new DictMsg("Put", "stale", "x", 0), 1);
        assertTrue(follower.log.installSnapshot(snap));
        assertEquals(n - 1, follower.log.getCommitIdx());
        assertEquals(n - 1, follower.log.getLastIdx());
        assertEquals(leaderKv.store, followerKv.store);
        assertTrue(Files.exists(follower.log.getSnapshotPath()));

        // installing the same snapshot again is a no-op
        assertFalse(follower.log.installSnapshot(snap));
    }

    @Test
    void clusterStateRoundTripsThroughSnapshot() throws Exception {
        Map<String, Node> seeds = config();
        ClusterState original = new ClusterState(new Pipe(), seeds, "snapTestA",
            new Pipe(), new Pipe(), new Pipe(), new Pipe());
        for (int i = 0; i < 6; i++)
            original.apply(new NodeMsg("Add", new Node("extra" + i, "localhost", 100 + i), 0));
        original.apply(new NodeMsg("Remove", "extra2", 0));

        Snapshot snap = new Snapshot(0, 0, 0, null, null, null, 0);
        original.fillSnapshot(snap);
        assertEquals(7, snap.membershipOps.size());

        ClusterState restored = new ClusterState(new Pipe(), seeds, "snapTestA",
            new Pipe(), new Pipe(), new Pipe(), new Pipe());
        restored.restore(snap);

        Snapshot again = new Snapshot(0, 0, 0, null, null, null, 0);
        restored.fillSnapshot(again);
        assertEquals(snap.membershipOps.size(), again.membershipOps.size());
        assertEquals(configJson(original), configJson(restored));
    }

    /** Serializes the cluster view the same way Config replies do. */
    private static String configJson(ClusterState cs) throws Exception {
        java.lang.reflect.Field cluster = ClusterState.class.getDeclaredField("cluster");
        java.lang.reflect.Field version = ClusterState.class.getDeclaredField("version");
        cluster.setAccessible(true);
        version.setAccessible(true);
        return new com.google.gson.Gson().toJson(cluster.get(cs)) + "|" + version.get(cs);
    }
}
