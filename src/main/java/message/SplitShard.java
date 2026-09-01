package message;

import java.util.Map;
import cluster.Node;
import cluster.ConsistentHashMap;

/**
 * SplitShard
 * the message used to signify a raft cluster of a shard spliting event
 */
public class SplitShard extends Update {
    /** full cluster map carried for redistribution workflow. */
    public ConsistentHashMap cluster;

    /** holds the nodes that will make up the new shard */
    public Map<String, Node> newNodes;

    /**
     * constructs a SplitShard message for split shard events
     * 
     * @param nodes the node configuration of the inital shard after split event
     * @param newNodes the node configuration of the new shard after split event
     * @param version the cluster configuration version
     */
    public SplitShard(Map<String, Node> nodes, Map<String, Node> newNodes, int version) {
        super("SplitShard", nodes,version);
        this.newNodes = newNodes;
    }
}
