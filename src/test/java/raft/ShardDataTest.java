package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import cluster.ConsistentHashMap;
import cluster.Node;
import cluster.Shard;
import communication.Pipe;
import message.DictMsg;
import message.Message;
import message.SplitRaftConfig;
import message.Snapshot;
import message.Update;

class ShardDataTest {

    /** Key-value state machine with the split/merge hooks. */
    static class Kv implements ReplicatedStateMachine {
        final Map<String, String> store = new HashMap<>();
        public void apply(Message msg) {
            DictMsg d = (DictMsg) msg;
            if (d.action.equals("Put")) store.put(d.key, d.value);
        }
        public void fillSnapshot(Snapshot s) { s.kvStore = new HashMap<>(store); }
        public void restore(Snapshot s) { store.clear(); store.putAll(s.kvStore); }
        public Map<String, String> exportKeys() { return new HashMap<>(store); }
        public void retainKeys(Predicate<String> keep) { store.keySet().removeIf(k -> !keep.test(k)); }
    }

    /** Node ids whose RaftState may have written a snapshot file. */
    private final java.util.Set<String> used = new java.util.HashSet<>();

    @AfterEach
    void cleanup() throws Exception {
        for (String id : used)
            java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(RaftLog.SNAPSHOT_DIR, id + "-Shard.json"));
    }

    private static Map<String, Node> nodes(String prefix, int count, int port) {
        Map<String, Node> m = new HashMap<>();
        for (int i = 0; i < count; i++)
            m.put(prefix + i, new Node(prefix + i, "localhost", port + i));
        return m;
    }

    @Test
    void ownerOfMatchesRing() throws Exception {
        ConsistentHashMap ring = new ConsistentHashMap();
        for (Node n : nodes("N", 14, 5000).values())
            ring.addNode(n);
        TreeMap<Long, String> owners = ring.ringOwners();
        for (int i = 0; i < 500; i++)
            assertEquals(ring.getShard("key" + i).id, ConsistentHashMap.ownerOf(owners, "key" + i));
    }

    @Test
    void splitKeepsOnlyOwnedKeysOnEachHalf() throws Exception {
        // build a ring and grow shard0 until it splits
        ConsistentHashMap ring = new ConsistentHashMap();
        Map<String, Shard> result = null;
        int i = 0;
        while (result == null || !result.containsKey("new"))
            result = ring.addNode(new Node("N" + i, "localhost", 6000 + i++));
        Map<String, Node> inNodes = result.get("old").getAllNodes();
        Map<String, Node> newNodes = result.get("new").getAllNodes();
        Map<String, Node> all = new HashMap<>(inNodes);
        all.putAll(newNodes);
        TreeMap<Long, String> owners = ring.ringOwners();
        String inId = result.get("old").id, newId = result.get("new").id;

        int movedKeys = 0;
        for (String side : new String[] {"in", "new"}) {
            String self = (side.equals("in") ? inNodes : newNodes).keySet().iterator().next();
            used.add(self);
            Kv kv = new Kv();
            RaftState state = new RaftState(self, kv, "Shard", all, new Pipe(), false);
            for (int k = 0; k < 200; k++)
                state.log.appendEntry(new DictMsg("Put", "key" + k, "v" + k, 0), 1);
            state.log.appendEntry(new SplitRaftConfig(all, inNodes, newNodes, null, false, 1, owners, inId, newId), 1);
            state.log.commitEntries(state.log.getLastIdx());

            String myShard = side.equals("in") ? inId : newId;
            for (String key : kv.store.keySet())
                assertEquals(myShard, ConsistentHashMap.ownerOf(owners, key));
            if (side.equals("new"))
                movedKeys = kv.store.size();
            else
                assertEquals(200 - kv.store.size(), countOwnedBy(owners, newId));
        }
        assertEquals(countOwnedBy(owners, newId), movedKeys);
        assertTrue(movedKeys > 0, "test ring should move some keys to the new shard");
    }

    private static int countOwnedBy(TreeMap<Long, String> owners, String shard) throws Exception {
        int c = 0;
        for (int k = 0; k < 200; k++)
            if (shard.equals(ConsistentHashMap.ownerOf(owners, "key" + k))) c++;
        return c;
    }

    @Test
    void shardRemovalHandsKeysOverAndClearsStore() throws Exception {
        ConsistentHashMap ring = new ConsistentHashMap();
        for (Node n : nodes("M", 9, 7000).values())
            ring.addNode(n);
        Map<String, Node> oldShard = new HashMap<>();
        oldShard.put("gone", new Node("gone", "localhost", 7999));

        used.add("gone");
        Kv kv = new Kv();
        RaftState state = new RaftState("gone", kv, "Shard", oldShard, new Pipe(), false);
        for (int k = 0; k < 50; k++)
            state.log.appendEntry(new DictMsg("Put", "key" + k, "v" + k, 0), 1);
        state.log.commitEntries(state.log.getLastIdx());
        assertEquals(50, kv.store.size());

        Map<String, Node> newShard = ring.getShardWithNode("M0").getAllNodes();
        newShard.put("gone", oldShard.get("gone"));
        state.shardRemoval(new Update(newShard, 2, ring.ringOwners(), ring.shardMembers()));

        assertTrue(kv.store.isEmpty());
        assertEquals("learner", state.type);
        assertEquals(-1, state.log.getLastIdx());
        // one hand-over per shard that owns some of the keys, all still unacknowledged
        long owningShards = 0;
        Map<String, Boolean> seen = new HashMap<>();
        for (int k = 0; k < 50; k++)
            seen.put(ConsistentHashMap.ownerOf(ring.ringOwners(), "key" + k), true);
        owningShards = seen.size();
        assertEquals(owningShards, state.pendingMigrations.size());
    }
}
