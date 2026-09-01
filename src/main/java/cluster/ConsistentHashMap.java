package cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.TreeMap;
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
 */
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
     */
    private void removeShard(Shard shard) throws Exception {
        for (int i = 0; i < virtualShards; i++)
            ring.remove(Hash(shard.id+i));

        List<Node> leftOver = shard.getLeft();
        for (Node n : leftOver) {
            addNode(n);
        }
        numShards--;
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
        Shard shard = getShardWithNode(id);
        if (shard != null) {
            shard.removeNode(id);
            if (shard.length < minShardSize && ring.size() > 1)
                removeShard(shard);
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
        long hash = 0;
        MessageDigest md = MessageDigest.getInstance("MD5");

        md.update(s.getBytes(StandardCharsets.UTF_8));
        byte[] digest = md.digest();

        for (int i = 0; i < 8; i++)
            hash = (hash << 8) | (long) ((digest[i] & 0xff));
        return hash;
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
}