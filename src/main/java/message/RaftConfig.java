package message;

import cluster.Node;
import java.util.Map;

/**
 * Carries a cluster-node configuration through the Raft message pipeline.
 */
public class RaftConfig extends Message {
    /** Nodes that make up the configuration being replicated. */
    public Map<String, Node> nodes;
    public Map<String, Node> oldNodes;
    public boolean jointConfig;
    public int version;

    /**
     * Creates a Raft configuration message.
     *
     * @param nodes node configuration to replicate
     * @param oldNodes previous configuration for joint consensus
     * @param jointConfig whether this is a joint-config transition
     * @param version raft message version
     */
    public RaftConfig(Map<String, Node> nodes, Map<String, Node> oldNodes, boolean jointConfig, int version) {
        super("RaftConfig");
        this.nodes = nodes;
        this.oldNodes = oldNodes;
        this.jointConfig = jointConfig;
        this.version = version;
    }
}
