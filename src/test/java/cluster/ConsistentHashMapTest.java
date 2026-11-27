package cluster;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class ConsistentHashMapTest {

    private static ConsistentHashMap withNodes(int count) throws Exception {
        ConsistentHashMap map = new ConsistentHashMap();
        for (int i = 0; i < count; i++)
            map.addNode(new Node("N" + i, "localhost", 1000 + i));
        return map;
    }

    @Test
    void startsWithOneEmptyShard() throws Exception {
        ConsistentHashMap map = new ConsistentHashMap();
        assertEquals(1, map.numShards);
        assertTrue(map.getAllNodes().isEmpty());
        assertEquals("shard0", map.getShard("anything").id);
        assertEquals(3, map.ringOwners().size()); // virtual positions
    }

    @Test
    void splitsWhenAShardReachesSixNodes() throws Exception {
        ConsistentHashMap map = withNodes(5);
        assertEquals(1, map.numShards);

        Map<String, Shard> result = map.addNode(new Node("N5", "localhost", 1005));
        assertTrue(result.containsKey("new"));
        assertEquals(2, map.numShards);
        assertEquals(3, result.get("old").size());
        assertEquals(3, result.get("new").size());
        assertEquals(6, map.getAllNodes().size());
        assertEquals(6, map.ringOwners().size());
        assertEquals(2, map.shardMembers().size());
        assertSame(result.get("new"), map.getShardWithId(result.get("new").id));
    }

    @Test
    void keyLookupIsDeterministicAndMatchesOwnerOf() throws Exception {
        ConsistentHashMap map = withNodes(12);
        TreeMap<Long, String> owners = map.ringOwners();
        for (int i = 0; i < 200; i++) {
            String key = "key" + i;
            assertEquals(map.getShard(key).id, map.getShard(key).id);
            assertEquals(map.getShard(key).id, ConsistentHashMap.ownerOf(owners, key));
        }
        assertNull(ConsistentHashMap.ownerOf(new TreeMap<>(), "x"));
    }

    @Test
    void findsShardByNode() throws Exception {
        ConsistentHashMap map = withNodes(6);
        for (Node n : map.getAllNodes().values())
            assertTrue(map.getShardWithNode(n.id).contains(n.id));
        assertNull(map.getShardWithNode("missing"));
        assertNull(map.getShardWithId("missing"));
    }

    @Test
    void removingANodeShrinksItsShard() throws Exception {
        ConsistentHashMap map = withNodes(4);
        Shard s = map.removeNode("N2");
        assertNotNull(s);
        assertEquals(3, map.getAllNodes().size());
        assertNull(map.getShardWithNode("N2"));
        assertNull(map.removeNode("missing"));
    }

    @Test
    void removingBelowMinimumMergesShardIntoOthers() throws Exception {
        ConsistentHashMap map = withNodes(6);
        assertEquals(2, map.numShards);
        String victimShard = map.getShardWithNode("N0").id;
        Shard other = null;
        for (Map.Entry<String, Map<String, Node>> e : map.shardMembers().entrySet())
            if (!e.getKey().equals(victimShard))
                other = map.getShardWithId(e.getKey());

        map.removeNode("N0");

        assertEquals(1, map.numShards);
        assertEquals(5, map.getAllNodes().size());
        assertNull(map.getShardWithId(victimShard));
        assertEquals(5, other.size());
        assertEquals(3, map.ringOwners().size());
    }

    @Test
    void jsonListsEachNodeOnceAndRebuildsTheSameRing() throws Exception {
        ConsistentHashMap map = withNodes(8); // splits into two shards
        com.google.gson.Gson gson = new com.google.gson.Gson();
        String json = gson.toJson(map);
        for (Node n : map.getAllNodes().values()) {
            String addr = "\"port\":" + n.port + "}";
            assertEquals(json.indexOf(addr), json.lastIndexOf(addr), n.id + " repeated in " + json);
        }

        ConsistentHashMap copy = gson.fromJson(json, ConsistentHashMap.class);
        assertEquals(map.ringOwners(), copy.ringOwners());
        assertEquals(map.shardMembers().keySet(), copy.shardMembers().keySet());
        for (String shard : map.shardMembers().keySet())
            assertEquals(map.shardMembers().get(shard).keySet(), copy.shardMembers().get(shard).keySet());
        assertEquals(map.numShards, copy.numShards);
        for (int i = 0; i < 50; i++)
            assertEquals(map.getShard("key" + i).id, copy.getShard("key" + i).id);

        // left/right order and the shard counter survive: both evolve identically
        for (int i = 100; i < 106; i++) {
            Node n = new Node("X" + i, "localhost", 9000 + i);
            map.addNode(n);
            copy.addNode(n);
        }
        copy.removeNode("X100");
        map.removeNode("X100");
        assertEquals(map.ringOwners(), copy.ringOwners());
        for (String shard : map.shardMembers().keySet())
            assertEquals(map.shardMembers().get(shard).keySet(), copy.shardMembers().get(shard).keySet());
    }

    @Test
    void hashIsStable() throws Exception {
        ConsistentHashMap map = new ConsistentHashMap();
        assertEquals(ConsistentHashMap.hashOf("abc"), map.Hash("abc"));
        assertEquals(ConsistentHashMap.hashOf("abc"), ConsistentHashMap.hashOf("abc"));
        assertNotEquals(ConsistentHashMap.hashOf("abc"), ConsistentHashMap.hashOf("abd"));
    }
}
