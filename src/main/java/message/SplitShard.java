package message;

import java.util.Map;
import java.util.TreeMap;
import cluster.Node;

/**
 * SplitShard
 * the message used to signify a raft cluster of a shard spliting event
 */
public class SplitShard extends Update {
    /** holds the nodes that will make up the new shard */
    public Map<String, Node> newNodes;

    /** ring after the split (position to shard id), used to decide which keys each half keeps */
    public TreeMap<Long, String> ring;

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
        this.newNodes = newNodes;
        this.ring = ring;
        this.inShardId = inShardId;
        this.newShardId = newShardId;
    }
}
