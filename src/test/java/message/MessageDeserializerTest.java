package message;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import cluster.ConsistentHashMap;
import cluster.Node;
import testutil.Capture;

class MessageDeserializerTest {

    private static final Gson PLAIN = new Gson();

    private static Message roundTrip(Message m) {
        return Capture.GSON.fromJson(PLAIN.toJson(m), Message.class);
    }

    @Test
    void everyWireMessageRoundTripsToItsConcreteType() throws Exception {
        Node n = new Node("N1", "localhost", 1);
        Map<String, Node> nodes = Map.of("N1", n);
        TreeMap<Long, String> ring = new TreeMap<>(Map.of(5L, "shard0"));
        Snapshot snap = new Snapshot(9, 2, 3, "N1", nodes, nodes, 1);
        snap.kvStore.put("k", "v");

        List<Message> messages = List.of(
            new AppendEntries("Shard", 1, "N1", 0, 1, 0, null, 2),
            new AppendEntriesReply("Shard", 1, "N1", true, 4, 2),
            new DictMsg("Put", "k", "v", 1, n),
            new NodeMsg("Add", n, 1, n),
            new RequestVote("Cluster", 2, "N1", 3, 1, 0),
            new RequestVoteReply("Cluster", 2, "N1", true, 0),
            new RaftConfig(nodes, null, false, 1),
            new SplitRaftConfig(nodes, nodes, nodes, null, true, 1, ring, "shard0", "shard1"),
            new Config(n),
            new Config(new ConsistentHashMap(), 3),
            new Update(nodes, 1, false),
            new SplitShard(nodes, nodes, 1, ring, "shard0", "shard1"),
            new Ack(true, "ok", n),
            new Response("hello"),
            new InstallSnapshot("Shard", 3, "N1", snap, 1),
            snap,
            new MigrateData("m1", "shard0", Map.of("a", "b"), n),
            new MigrateAck("m1")
        );

        for (Message original : messages) {
            Message parsed = roundTrip(original);
            assertEquals(original.getClass(), parsed.getClass(), "type " + original.type);
            assertEquals(original.type, parsed.type);
            assertEquals(original.version, parsed.version);
        }
    }

    @Test
    void preservesFieldsOfRaftAndSnapshotMessages() {
        Map<String, Node> nodes = Map.of("N1", new Node("N1", "localhost", 1));
        Snapshot snap = new Snapshot(9, 2, 3, "N1", nodes, nodes, 1);
        snap.kvStore.put("k", "v");
        snap.membershipOps.add(new Snapshot.MembershipOp("Add", new Node("N2", "localhost", 2)));

        InstallSnapshot parsed = (InstallSnapshot) roundTrip(new InstallSnapshot("Shard", 3, "N1", snap, 1));
        assertEquals("Shard", parsed.level);
        assertEquals("N1", parsed.senderId);
        assertEquals(9, parsed.snapshot.lastIncludedIndex);
        assertEquals("v", parsed.snapshot.kvStore.get("k"));
        assertEquals("N2", parsed.snapshot.membershipOps.get(0).node.id);

        TreeMap<Long, String> ring = new TreeMap<>(Map.of(-7L, "shard0", 42L, "shard1"));
        SplitRaftConfig split = (SplitRaftConfig) roundTrip(
            new SplitRaftConfig(nodes, nodes, new HashMap<>(), null, false, 1, ring, "shard0", "shard1"));
        assertEquals(ring, split.ring);
        assertEquals("shard1", split.newShardId);
    }

    /** Number of times a substring appears in a string. */
    private static int count(String s, String sub) {
        int n = 0;
        for (int i = s.indexOf(sub); i >= 0; i = s.indexOf(sub, i + 1))
            n++;
        return n;
    }

    private static Map<String, Node> group(int... ports) {
        Map<String, Node> g = new HashMap<>();
        for (int p : ports)
            g.put("N" + p, new Node("N" + p, "localhost", 7000 + p));
        return g;
    }

    /** Each node's address must appear exactly once in the JSON, however many groups it is in. */
    private static void assertEachNodeOnce(String json, Map<String, Node> nodes) {
        for (Node n : nodes.values())
            assertEquals(1, count(json, "\"port\":" + n.port), n.id + " repeated in " + json);
    }

    @Test
    void overlappingNodeGroupsAreStoredOnceAndResolvedBack() {
        Map<String, Node> oldConfig = group(1, 2, 3, 4);
        Map<String, Node> newConfig = group(1, 2, 3, 4, 5, 6);
        Map<String, Node> in = group(1, 2, 3);
        Map<String, Node> fin = group(4, 5, 6);

        SplitRaftConfig split = new SplitRaftConfig(newConfig, in, fin, oldConfig, true, 2,
            new TreeMap<>(Map.of(1L, "shard0")), "shard0", "shard1");
        String json = PLAIN.toJson(split);
        assertEachNodeOnce(json, newConfig);

        SplitRaftConfig parsed = (SplitRaftConfig) Capture.GSON.fromJson(json, Message.class);
        assertEquals(newConfig.keySet(), parsed.nodes().keySet());
        assertEquals(oldConfig.keySet(), parsed.oldNodes().keySet());
        assertEquals(in.keySet(), parsed.inNodes().keySet());
        assertEquals(fin.keySet(), parsed.newNodes().keySet());
        assertEquals(7005, parsed.newNodes().get("N5").port);
        assertEquals("localhost", parsed.inNodes().get("N1").ip);

        // a final config has no old configuration
        RaftConfig fin2 = (RaftConfig) roundTrip(new RaftConfig(newConfig, null, false, 3));
        assertNull(fin2.oldNodes());
        assertEquals(newConfig.keySet(), fin2.nodes().keySet());
    }

    @Test
    void shardRemovalUpdateListsEachNodeOnce() {
        Map<String, Node> shard0 = group(1, 2, 3);
        Map<String, Node> shard1 = group(4, 5, 6);
        Map<String, Node> joining = group(1, 2, 3, 7);
        Map<String, Map<String, Node>> members = Map.of("shard0", shard0, "shard1", shard1);

        Update u = new Update(joining, 4, new TreeMap<>(Map.of(1L, "shard0")), members);
        String json = PLAIN.toJson(u);
        Map<String, Node> all = new HashMap<>(joining);
        all.putAll(shard1);
        assertEachNodeOnce(json, all);

        Update parsed = (Update) Capture.GSON.fromJson(json, Message.class);
        assertTrue(parsed.shardRemoved);
        assertEquals(joining.keySet(), parsed.nodes().keySet());
        assertEquals(shard0.keySet(), parsed.shardMembers().get("shard0").keySet());
        assertEquals(7006, parsed.shardMembers().get("shard1").get("N6").port);

        SplitShard ss = (SplitShard) roundTrip(new SplitShard(shard0, shard1, 2, new TreeMap<>(), "shard0", "shard1"));
        assertEquals(shard0.keySet(), ss.nodes().keySet());
        assertEquals(shard1.keySet(), ss.newNodes().keySet());
        assertNull(ss.shardMembers());
    }

    @Test
    void snapshotStoresVotersAsIdsAndStillReadsOldFiles() {
        Map<String, Node> active = group(1, 2, 3, 4);
        Map<String, Node> voters = group(1, 2, 3);
        Snapshot snap = new Snapshot(9, 2, 3, null, voters, active, 1);
        String json = PLAIN.toJson(snap);
        assertEachNodeOnce(json, active);

        Snapshot parsed = PLAIN.fromJson(json, Snapshot.class);
        assertEquals(voters.keySet(), parsed.voters().keySet());
        assertEquals(active.keySet(), parsed.activeNodes().keySet());

        // a snapshot file written before ids were introduced
        String legacy = "{\"type\":\"Snapshot\",\"version\":1,\"lastIncludedIndex\":4,"
            + "\"voters\":{\"A\":{\"id\":\"A\",\"ip\":\"localhost\",\"port\":1}},"
            + "\"activeNodes\":{\"A\":{\"id\":\"A\",\"ip\":\"localhost\",\"port\":1},"
            + "\"B\":{\"id\":\"B\",\"ip\":\"localhost\",\"port\":2}}}";
        Snapshot old = PLAIN.fromJson(legacy, Snapshot.class);
        assertEquals(1, old.voters().size());
        assertEquals(2, old.activeNodes().get("B").port);

        // messages without node groups leave the node list out entirely
        assertFalse(PLAIN.toJson(new Response("x")).contains("allNodes"));
    }

    @Test
    void deserializesPolymorphicLogEntryPayloads() {
        String json = "{\"type\":\"AppendEntries\",\"level\":\"Shard\",\"senderId\":\"L\",\"version\":0,"
            + "\"term\":1,\"prevLogIndex\":-1,\"prevLogTerm\":0,\"leaderCommit\":-1,\"entries\":["
            + "{\"msg\":{\"type\":\"DictMsg\",\"action\":\"Put\",\"key\":\"k\",\"value\":\"v\",\"version\":0},\"term\":1,\"index\":0},"
            + "{\"msg\":{\"type\":\"RaftConfig\",\"jointConfig\":true,\"version\":2},\"term\":1,\"index\":1}]}";
        AppendEntries ae = (AppendEntries) Capture.GSON.fromJson(json, Message.class);
        assertTrue(ae.entries.get(0).msg instanceof DictMsg);
        assertEquals("k", ((DictMsg) ae.entries.get(0).msg).key);
        assertTrue(ae.entries.get(1).msg instanceof RaftConfig);
        assertTrue(((RaftConfig) ae.entries.get(1).msg).jointConfig);
    }

    @Test
    void rejectsUnknownTypes() {
        assertThrows(JsonParseException.class,
            () -> Capture.GSON.fromJson("{\"type\":\"Bogus\"}", Message.class));
    }
}
