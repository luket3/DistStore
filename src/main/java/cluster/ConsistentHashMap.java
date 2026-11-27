package cluster;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.JsonAdapter;
import java.security.MessageDigest;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Maintains a consistent-hash ring that maps a logical key to a shard
 * abstraction.
 *
 * Each real shard contributes several virtual ring positions so that the
 * cluster can rebalance membership without a full remap of the entire ring.
 * The class also implements the local split and merge policy used for shard
 * resizing in the prototype.
 *
 * In JSON (e.g. a Config reply) the ring is written compactly by
 * {@link Json}: every node once, and each shard once as ordered lists of node
 * ids. Ring positions are not written, since each shard's virtual positions
 * are always derived from its id.
 */
@JsonAdapter(ConsistentHashMap.Json.class)
public class ConsistentHashMap {
    /**
     * Ring mapping a 64-bit hash value to the shard metadata instance that
     * owns that virtual position.
     */
    private final TreeMap<Long, Shard> ring;

    /**
     * Number of virtual replicas to place on the ring for each logical shard.
     */
    private static final int virtualShards = 3;

    /**
     * Minimum shard size used by the simple split/merge policy.
     */
    private static final int minShardSize = 3;

    /**
     * Monotonically increasing identifier used when creating a new shard.
     */
    private int currShardId = 0;

    /**
     * Number of logical shards currently present in the ring.
     */
    public int numShards = 0;

    /**
     * Add a new shard to the ring. If {@code shard} is {@code null} a new
     * empty {@link Shard} is allocated and placed on the ring.
     */
    private void addShard(Shard shard) throws Exception {

        if (shard == null)
            shard = new Shard("shard"+currShardId, minShardSize);

        for (int i = 0; i < virtualShards; i++)
            ring.put(Hash("shard"+currShardId+i),shard);
        currShardId++;
        numShards++;
    }

    /**
     * Remove a shard from the ring and redistribute its leftover nodes.
     *
     * Re-adding a leftover node can make the receiving shard big enough to
     * split; each such split is recorded in {@code splits}.
     *
     * @param shard shard to remove
     * @param splits receives one {old shard, new shard} pair per split caused
     *     by re-adding the leftover nodes
     */
    private void removeShard(Shard shard, List<Shard[]> splits) throws Exception {
        for (int i = 0; i < virtualShards; i++)
            ring.remove(Hash(shard.id+i));

        List<Node> leftOver = new ArrayList<>(shard.getLeft());
        leftOver.addAll(shard.getRight());
        for (Node n : leftOver) {
            Map<String, Shard> result = addNode(n);
            if (result.containsKey("new"))
                splits.add(new Shard[] {result.get("old"), result.get("new")});
        }
        numShards--;
    }

    /**
     * Creates a ring with no shards, to be filled in by {@link Json}.
     *
     * @param currShardId the number to give the next shard created
     */
    private ConsistentHashMap(int currShardId) {
        ring = new TreeMap<>();
        this.currShardId = currShardId;
    }

    /**
     * Create an empty consistent-hash ring and allocate the initial shard.
     */
    public ConsistentHashMap() throws Exception {
        ring = new TreeMap<>();
        addShard(null);
    }

    /**
     * Add a {@link Node} to its corresponding shard (by node id).
     *
     * @param n node to add
     * @throws Exception on errors during shard splitting or modification
     * @return if a shard is split, return the new shard created
     */
    public Map<String, Shard> addNode(Node n) throws Exception {
        Shard shard = getShard(n.id);
        shard.addNode(n);

        Map<String, Shard> result = new HashMap<>();
        result.put("old", shard);
        if (shard.length >= minShardSize*2) {
            result.put("new", shard.split("shard"+currShardId));
            addShard(result.get("new"));
        }
        return result;
    }

    /**
     * Remove a node from the ring by id and redistribute any leftover nodes.
     *
     * @param id identifier of the node to remove
     * @throws Exception on errors during shard merging or modification
     * @return the shard which the node was removed from
     */
    public Shard removeNode(String id) throws Exception {
        return removeNode(id, new ArrayList<>());
    }

    /**
     * Remove a node from the ring by id and redistribute any leftover nodes,
     * reporting any shard splits the redistribution causes.
     *
     * @param id identifier of the node to remove
     * @param splits receives one {old shard, new shard} pair per split caused
     *     by redistributing a removed shard's leftover nodes
     * @throws Exception on errors during shard merging or modification
     * @return the shard which the node was removed from
     */
    public Shard removeNode(String id, List<Shard[]> splits) throws Exception {
        Shard shard = getShardWithNode(id);
        if (shard != null) {
            shard.removeNode(id);
            if (shard.length < minShardSize && ring.size() > 1)
                removeShard(shard, splits);
        }
        return shard;
    }

    /**
     * Return the shard responsible for the supplied key using consistent
     * hashing.
     *
     * @param key the key to look up
     * @return the {@link Shard} responsible for {@code key}, or {@code null}
     * if the ring is empty
     * @throws Exception on hashing errors
     */
    public Shard getShard(String key) throws Exception {
        if (ring.size() < 1)
            return null;

        NavigableMap<Long, Shard> tailMap = ring.tailMap(Hash(key),true);
        if (tailMap.size() >= 1)
            return tailMap.firstEntry().getValue();
        else
            return ring.firstEntry().getValue();
    }

    /**
     * Find the shard that contains the specified node id.
     *
     * @param id the node id to search for
     * @return the shard containing the node, or null if not found
     */
    public Shard getShardWithNode(String id) {
        for (Shard shard : ring.values())
            if (shard.contains(id))
                return shard;

        return null;
    }

    /**
     * Find the shard with the specified shard id.
     *
     * @param shardId the shard id to search for
     * @return the shard with the given id, or null if not found
     */
    public Shard getShardWithId(String shardId) {
        for (Shard shard : ring.values())
            if (shard.id.equals(shardId))
                return shard;

        return null;
    }

    /**
     * Returns the unique node set represented by all shards on the ring.
     *
     * @return a map of node identifier to node instance for the current ring
     */
    public Map<String, Node> getAllNodes() {
        Map<String, Node> nodes = new HashMap<>();
        HashSet<Shard> visitedShards = new HashSet<>();
    
        for (Shard shard : ring.values()) {
            if (!visitedShards.contains(shard)) {
                visitedShards.add(shard);
                for (Node n : shard.getAllNodes().values()) {
                nodes.put(n.id, n);
                }
            }
        }
        return nodes;
    }

    /**
     * Compute a 64-bit value from the MD5 digest of the provided string.
     *
     * @param s input string
     * @return 64-bit hash derived from MD5(s)
     * @throws Exception if the MD5 MessageDigest cannot be obtained
     */
    public long Hash(String s) throws Exception {
        return hashOf(s);
    }

    /**
     * Compute a 64-bit value from the MD5 digest of the provided string.
     *
     * @param s input string
     * @return 64-bit hash derived from MD5(s)
     * @throws Exception if the MD5 MessageDigest cannot be obtained
     */
    public static long hashOf(String s) throws Exception {
        long hash = 0;
        MessageDigest md = MessageDigest.getInstance("MD5");

        md.update(s.getBytes(StandardCharsets.UTF_8));
        byte[] digest = md.digest();

        for (int i = 0; i < 8; i++)
            hash = (hash << 8) | (long) ((digest[i] & 0xff));
        return hash;
    }

    /**
     * Returns the ring as a map from each virtual position to the id of the
     * shard that owns it. This is enough to work out which shard owns any key
     * with {@link #ownerOf}, and unlike the full ring it serializes cleanly.
     *
     * @return ring position to shard id
     */
    public TreeMap<Long, String> ringOwners() {
        TreeMap<Long, String> owners = new TreeMap<>();
        for (Map.Entry<Long, Shard> e : ring.entrySet())
            owners.put(e.getKey(), e.getValue().id);
        return owners;
    }

    /**
     * Returns the members of every shard, keyed by shard id.
     *
     * @return shard id to that shard's nodes
     */
    public Map<String, Map<String, Node>> shardMembers() {
        Map<String, Map<String, Node>> members = new HashMap<>();
        for (Shard shard : ring.values())
            members.put(shard.id, shard.getAllNodes());
        return members;
    }

    /**
     * Returns the id of the shard that owns a key, using a ring produced by
     * {@link #ringOwners}. Matches {@link #getShard} on the original ring.
     *
     * @param ring ring position to shard id
     * @param key key to look up
     * @return owning shard id, or null if the ring is empty
     * @throws Exception on hashing errors
     */
    public static String ownerOf(NavigableMap<Long, String> ring, String key) throws Exception {
        if (ring == null || ring.isEmpty())
            return null;
        Map.Entry<Long, String> e = ring.ceilingEntry(hashOf(key));
        return e != null ? e.getValue() : ring.firstEntry().getValue();
    }

    /**
     * Print a human-readable representation of the ring and its shards to
     * stdout.
     */
    public void print() {
        for (Map.Entry<Long, Shard> e : ring.entrySet()) {
            System.out.print(e.getValue().id + " ");

            List<Node> left = e.getValue().getLeft();
            for (Node n : left) {
                System.out.print(n.id + " ");
            }

            List<Node> right = e.getValue().getRight();
            for (Node n : right) {
                System.out.print(n.id + " ");
            }

            System.out.println();
        }
    }

    /**
     * Writes and reads a ring in a compact JSON form:
     * <pre>
     * {"nodes":[{"id":"A","ip":"localhost","port":4567}, ...],
     *  "shards":[{"id":"shard0","left":["A","B","C"],"right":["D"]}, ...],
     *  "currShardId":2}
     * </pre>
     * Each shard sits at several virtual ring positions, so writing the ring
     * itself would repeat every shard, and every node address in it, once per
     * position. Positions are recomputed from the shard ids instead, the same
     * way {@link #addShard} and {@link #removeShard} compute them.
     */
    static class Json implements JsonSerializer<ConsistentHashMap>, JsonDeserializer<ConsistentHashMap> {

        @Override
        public JsonElement serialize(ConsistentHashMap map, Type type, JsonSerializationContext ctx) {
            Map<String, Node> nodes = new LinkedHashMap<>();
            JsonArray shards = new JsonArray();
            for (Shard shard : distinctShards(map)) {
                JsonObject s = new JsonObject();
                s.addProperty("id", shard.id);
                s.add("left", ids(shard.getLeft(), nodes));
                s.add("right", ids(shard.getRight(), nodes));
                shards.add(s);
            }

            JsonObject out = new JsonObject();
            out.add("nodes", ctx.serialize(new ArrayList<>(nodes.values())));
            out.add("shards", shards);
            out.addProperty("currShardId", map.currShardId);
            return out;
        }

        @Override
        public ConsistentHashMap deserialize(JsonElement json, Type type, JsonDeserializationContext ctx)
                throws JsonParseException {
            JsonObject in = json.getAsJsonObject();
            Map<String, Node> nodes = new HashMap<>();
            for (JsonElement e : in.getAsJsonArray("nodes")) {
                Node n = ctx.deserialize(e, Node.class);
                nodes.put(n.id, n);
            }

            ConsistentHashMap map = new ConsistentHashMap(in.get("currShardId").getAsInt());
            try {
                for (JsonElement e : in.getAsJsonArray("shards")) {
                    JsonObject s = e.getAsJsonObject();
                    Shard shard = new Shard(s.get("id").getAsString(), minShardSize,
                        lookup(s.getAsJsonArray("left"), nodes), lookup(s.getAsJsonArray("right"), nodes));
                    for (int i = 0; i < virtualShards; i++)
                        map.ring.put(map.Hash(shard.id + i), shard);
                    map.numShards++;
                }
            } catch (JsonParseException e) {
                throw e;
            } catch (Exception e) {
                throw new JsonParseException("cannot rebuild ring: " + e.getMessage(), e);
            }
            return map;
        }

        /** Each shard once, in ring order of its first position. */
        private static List<Shard> distinctShards(ConsistentHashMap map) {
            Map<String, Shard> shards = new LinkedHashMap<>();
            for (Shard shard : map.ring.values())
                shards.putIfAbsent(shard.id, shard);
            return new ArrayList<>(shards.values());
        }

        /** The ids of a node list, recording each node in the shared node table. */
        private static JsonArray ids(List<Node> list, Map<String, Node> table) {
            JsonArray ids = new JsonArray();
            for (Node n : list) {
                ids.add(n.id);
                table.putIfAbsent(n.id, n);
            }
            return ids;
        }

        /** The nodes named by a list of ids. */
        private static List<Node> lookup(JsonArray ids, Map<String, Node> table) {
            List<Node> list = new ArrayList<>();
            for (JsonElement id : ids) {
                Node n = table.get(id.getAsString());
                if (n == null)
                    throw new JsonParseException("ring refers to unknown node " + id.getAsString());
                list.add(n);
            }
            return list;
        }
    }
}
