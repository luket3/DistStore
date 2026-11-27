package message;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import cluster.Node;

/**
 * SplitRaftConfig
 * this class holds a raft config log entry for a split configuration event.
 * The two halves are stored as node ids into the message's node list.
 */
public class SplitRaftConfig extends RaftConfig {

    /** ids of the nodes left in the inital shard after the split */
    private Set<String> inNodeIds;

    /** ids of the nodes migrated to the new shard after the split */
    private Set<String> newNodeIds;

    /** ring after the split (position to shard id), used to decide which keys each half keeps */
    public TreeMap<Long, String> ring;

    /** id of the shard that split; the nodes in {@link #inNodes()} keep this id */
    public String inShardId;

    /** id of the shard created by the split; the nodes in {@link #newNodes()} take this id */
    public String newShardId;

    /**
     * 
     * @param nodes the node configuration to replicate
     * @param inNodes the nodes left in the inital shard after the split
     * @param newNodes the nodes migrated to the new shard after the split
     * @param oldNodes the previous configuration for joint consensus (may be null for initial configuration)
     * @param jointConfig whether this represents a joint-config transition
     * @param version the raft message version
     * @param ring ring after the split, as ring position to shard id
     * @param inShardId id of the shard that split
     * @param newShardId id of the shard created by the split
     */
    public SplitRaftConfig(Map<String, Node> nodes, Map<String, Node> inNodes, Map<String, Node> newNodes,
                           Map<String, Node> oldNodes, boolean jointConfig, int version,
                           TreeMap<Long, String> ring, String inShardId, String newShardId) {
        super("SplitRaftConfig", nodes, oldNodes, jointConfig, version);
        this.inNodeIds = ref(inNodes);
        this.newNodeIds = ref(newNodes);
        this.ring = ring;
        this.inShardId = inShardId;
        this.newShardId = newShardId;
    }

    /**
     * The nodes left in the initial shard after the split.
     *
     * @return a new map of the nodes keyed by id, or null if none were set
     */
    public Map<String, Node> inNodes() {
        return deref(inNodeIds);
    }

    /**
     * The nodes migrated to the new shard after the split.
     *
     * @return a new map of the nodes keyed by id, or null if none were set
     */
    public Map<String, Node> newNodes() {
        return deref(newNodeIds);
    }
}
