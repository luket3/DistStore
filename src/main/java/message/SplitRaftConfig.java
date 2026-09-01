package message;

import java.util.Map;
import cluster.Node;

/**
 * SplitRaftConfig
 * this class holds a raft config log entry for a split configuration event
 */
public class SplitRaftConfig extends RaftConfig {

    /** the nodes left in the inital shard after the split */
    public Map<String, Node> inNodes;

    /** the nodes migrated to the new shard after the split */
    public Map<String, Node> newNodes;
    
    /**
     * 
     * @param nodes the node configuration to replicate
     * @param inNodes the nodes left in the inital shard after the split
     * @param newNodes the nodes migrated to the new shard after the split
     * @param oldNodes the previous configuration for joint consensus (may be null for initial configuration)
     * @param jointConfig whether this represents a joint-config transition
     * @param version the raft message version
     */
    public SplitRaftConfig(Map<String, Node> nodes, Map<String, Node> inNodes, Map<String, Node> newNodes, 
                           Map<String, Node> oldNodes, boolean jointConfig, int version) {
        super("SplitRaftConfig", nodes, oldNodes, jointConfig, version);
        this.inNodes = inNodes;
        this.newNodes = newNodes;
    }
}
