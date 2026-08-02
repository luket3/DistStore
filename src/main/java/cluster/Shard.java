package cluster;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Represents the logical node subset assigned to one shard on the ring.
 *
 * The shard keeps two ordered node lists, {@code left} and {@code right},
 * which are used by the prototype's simple split policy. A shard is expected
 * to grow until it reaches twice the configured minimum size, at which point
 * the right-hand list can be promoted into a new shard.
 */
public class Shard {
    /** Nodes in the left side of the Shard */
    private LinkedList<Node> left;

    /** Nodes in the right side of the Shard */
    private LinkedList<Node> right;

    /** Minimum size for this shard */
    private int minShardSize = 3;

    /** Identifier for this shard */
    public String id;

    /** Current number of nodes in this shard */
    public int length;

    /**
     * Internal constructor used when creating a split shard with an initial
     * node list.
     */
    private Shard(String id, int minShardSize, LinkedList<Node> nodes) {
        this(id,minShardSize);
        this.left = nodes;
        this.length = nodes.size();
    }

    /**
     * Create an empty shard with the given id and minimum size threshold.
     */
    public Shard(String id, int minShardSize) {
        this.id = id;
        this.length = 0;
        this.minShardSize = minShardSize;

        this.left = new LinkedList<>();
        this.right = new LinkedList<>();
    }

    /**
     * Add a node to this shard. Nodes go to the left list until the shard
     * reaches {@code minShardSize}, after which nodes are appended to the
     * right list.
     */
    public void addNode(Node n) {
        if (length < minShardSize)
            left.add(n);
        else
            right.add(n);

        length++;
    }

    /**
     * Remove a node from this shard by id and rebalance left/right lists if
     * necessary.
     */
    public void removeNode(String id) {
        Iterator<Node> it = left.iterator();
        boolean leftSide = true;

        while (it.hasNext()) {
            if (it.next().id.equals(id)) {
                it.remove();
                break;
            }

            if (leftSide && !it.hasNext()) {
                it = right.iterator();
                leftSide = false;
            }
        }
        length--;

        if (left.size() < minShardSize && right.size() >= 1)
            left.add(right.removeLast());
    }

    /**
     * Split this shard into two when its size equals twice the minimum shard
     * size. The current shard keeps the left list; a new shard is created
     * containing the right list.
     *
     * @param newId id for the newly created shard
    * @return the newly created {@link Shard}, or {@code null} if splitting
    * is not applicable
     */
    public Shard split(String newId) {
        if (length != minShardSize*2)
            return null;

        length = minShardSize;
        Shard newShard = new Shard(newId, minShardSize, right);
        right = new LinkedList<>();
        return newShard;
    }

    /** Return nodes currently stored on the left side of the shard. */
    public List<Node> getLeft() {
        return left;
    }

    /** Return nodes currently stored on the right side of the shard. */
    public List<Node> getRight() {
        return right;
    }

    /** Return the first node in the left list, or null if empty. */
    public Node get(HashSet<String> killed) {
        if (killed == null)
            killed = new HashSet<>();

        for (Node n : left)
            if (!killed.contains(n.id))
                return n;
        for (Node n : right)
            if (!killed.contains(n.id))
                return n;

        return null;
    }

    /**
     * Check if this shard contains a node with the given id.
     *
     * @param id the node id to check for
     * @return true if the shard contains a node with the given id,
     *         false otherwise
     */
    public Boolean contains(String id) {
        for (Node n : left)
            if (n.id.equals(id))
                return true;
        for (Node n : right)
            if (n.id.equals(id))
                return true;

        return false;
    }

    /**
     * Get all nodes in this shard as a map from node id to node.
     *
     * @return a map containing all nodes in this shard
     */
    public HashMap<String, Node> getAllNodes() {
        HashMap<String, Node> allNodes = new HashMap<>();
        for (Node n : left) {
            allNodes.put(n.id, n);
        }
        for (Node n : right) {
            allNodes.put(n.id, n);
        }
        return allNodes;
    }

    public int size() {
        return length;
    }
}