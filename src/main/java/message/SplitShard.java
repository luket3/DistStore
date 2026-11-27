package message;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import cluster.Node;

/**
 * SplitShard
 * the message used to signify a raft cluster of a shard spliting event.
 * The inherited {@link Update#ring} holds the ring after the split (position
 * to shard id), used to decide which keys each half keeps.
 */
public class SplitShard extends Update {
    /** ids of the nodes that will make up the new shard */
    private Set<String> newNodeIds;

    /** id of the shard that split */
    public String inShardId;

    /** id of the shard created by the split */
    public String newShardId;

    /**
     * constructs a SplitShard message for split shard events
     *
     * @param nodes the node configuration of the inital shard after split event
     * @param newNodes the node configuration of the new shard after split event
     * @param version the cluster configuration version
     * @param ring ring after the split, as ring position to shard id
     * @param inShardId id of the shard that split
     * @param newShardId id of the shard created by the split
     */
    public SplitShard(Map<String, Node> nodes, Map<String, Node> newNodes, int version,
                      TreeMap<Long, String> ring, String inShardId, String newShardId) {
        super("SplitShard", nodes, version, false);
        this.newNodeIds = ref(newNodes);
        this.ring = ring;
        this.inShardId = inShardId;
        this.newShardId = newShardId;
    }

    /**
     * The nodes that will make up the new shard.
     *
     * @return a new map of the nodes keyed by id, or null if none were set
     */
    public Map<String, Node> newNodes() {
        return deref(newNodeIds);
    }
}
