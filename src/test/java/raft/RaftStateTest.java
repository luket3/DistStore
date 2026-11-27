package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import communication.Pipe;
import message.DictMsg;
import message.RaftConfig;
import message.Snapshot;
import testutil.KvFake;
import testutil.Nodes;

class RaftStateTest {

    private Map<String, Node> config;
    private RaftState state;

    @BeforeEach
    void setUp() {
        config = Nodes.map(Nodes.unreachable("A"), Nodes.unreachable("B"), Nodes.unreachable("C"));
        state = new RaftState("A", new KvFake(), "Shard", config, new Pipe(), false);
    }

    @AfterEach
    void tearDown() {
        Nodes.deleteSnapshots("A");
    }

    @Test
    void startsAsFollowerOrLearner() {
        assertEquals("follower", state.type);
        assertEquals(0, state.term);
        assertEquals(3, state.voters.size());
        assertEquals("learner", new RaftState("A", new KvFake(), "Shard", config, new Pipe(), true).type);
    }

    @Test
    void logFileDependsOnLevel() {
        assertEquals("logs/ShardRaft.log", state.getLogFilePath());
        assertEquals("logs/ClusterRaft.log",
            new RaftState("A", new KvFake(), "Cluster", config, new Pipe(), false).getLogFilePath());
    }

    @Test
    void leaderStateStartsAfterTheLog() {
        state.log.appendEntry(new DictMsg("Put", "k", "v", 0), 1);
        state.log.appendEntry(new DictMsg("Put", "k", "v", 0), 1);
        state.initializeLeaderState();
        for (String id : config.keySet()) {
            assertEquals(-1, state.matchIndex.get(id));
            assertEquals(2, state.nextIndex.get(id));
        }
    }

    @Test
    void pendingLogFlagIsConsumedOnce() {
        state.pendingLog = true;
        assertTrue(state.getPendingLog());
        assertFalse(state.getPendingLog());
    }

    @Test
    void stepDownKeepsLearnersAsLearners() {
        state.type = "leader";
        state.stepDown();
        assertEquals("follower", state.type);
        state.type = "learner";
        state.stepDown();
        assertEquals("learner", state.type);
    }

    @Test
    void largerConfigAddsNewNodesAsLearners() {
        Map<String, Node> bigger = new HashMap<>(config);
        bigger.put("D", Nodes.unreachable("D"));
        state.type = "leader";
        state.initializeLeaderState();

        state.readNewConfig(bigger, 1);

        assertTrue(state.activeNodes.containsKey("D"));
        assertTrue(state.learners.containsKey("D"));
        assertTrue(state.newLearners.containsKey("D"));
        assertFalse(state.voters.containsKey("D"));
        assertEquals(0, state.nextIndex.get("D"));
        assertEquals(1, state.nextVersion);
    }

    @Test
    void smallerConfigOnLeaderAppendsJointConfig() {
        state.type = "leader";
        state.initializeLeaderState();
        Map<String, Node> smaller = new HashMap<>(config);
        smaller.remove("C");

        state.readNewConfig(smaller, 1);

        assertTrue(state.configChangePending);
        assertEquals(0, state.log.getLastIdx());
        RaftConfig entry = (RaftConfig) state.log.get(0).msg;
        assertTrue(entry.jointConfig);
        assertEquals(smaller.keySet(), entry.nodes().keySet());
        assertTrue(state.getPendingLog());
    }

    @Test
    void jointThenFinalConfigRemovesNodes() {
        Map<String, Node> smaller = new HashMap<>(config);
        smaller.remove("C");

        state.proccessNewConfig(new RaftConfig(smaller, config, true, 1));
        assertTrue(state.jointConfig);
        assertEquals(3, state.voters.size());

        state.proccessNewConfig(new RaftConfig(smaller, null, false, 1));
        assertFalse(state.jointConfig);
        assertEquals(1, state.version);
        assertEquals(smaller.keySet(), state.voters.keySet());
        assertFalse(state.activeNodes.containsKey("C"));
        assertEquals("follower", state.type);

        // stale configs are ignored
        state.proccessNewConfig(new RaftConfig(config, null, false, 1));
        assertEquals(2, state.voters.size());
    }

    @Test
    void finalConfigWithoutSelfMakesNodeALearner() {
        Map<String, Node> withoutSelf = new HashMap<>(config);
        withoutSelf.remove("A");
        state.proccessNewConfig(new RaftConfig(withoutSelf, null, false, 1));
        assertEquals("learner", state.type);
    }

    @Test
    void snapshotConfigIsAppliedOnlyIfNotOlder() {
        Map<String, Node> withoutSelf = new HashMap<>(config);
        withoutSelf.remove("A");
        state.version = 2;

        state.applySnapshotConfig(new Snapshot(5, 1, 1, null, withoutSelf, withoutSelf, 1));
        assertEquals("follower", state.type);
        assertEquals(3, state.voters.size());

        state.applySnapshotConfig(new Snapshot(5, 1, 1, null, withoutSelf, withoutSelf, 3));
        assertEquals(3, state.version);
        assertEquals("learner", state.type);
        assertEquals(withoutSelf.keySet(), state.voters.keySet());
    }
}
