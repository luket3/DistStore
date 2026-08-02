package message;

import cluster.Node;
import java.util.Map;

/**
 * Carries a cluster-node configuration through the Raft message pipeline.
 * This message is used to replicate cluster configuration changes during
 * joint consensus transitions in the Raft consensus algorithm.
 */
public class RaftConfig extends Message {
    /** Nodes that make up the configuration being replicated. */
    public Map<String, Node> nodes;
    /** Previous configuration for joint consensus (used during cluster membership changes). */
    public Map<String, Node> oldNodes;
    /** Whether this is a joint-config transition. */
    public boolean jointConfig;
    /** Raft message version. */
    public int version;

    /**
     * Constructs a new Raft configuration message.
     *
     * @param nodes the node configuration to replicate
     * @param oldNodes the previous configuration for joint consensus (may be null for initial configuration)
     * @param jointConfig whether this represents a joint-config transition
     * @param version the raft message version
     */
    public RaftConfig(Map<String, Node> nodes, Map<String, Node> oldNodes, boolean jointConfig, int version) {
        super("RaftConfig");
        this.nodes = nodes;
        this.oldNodes = oldNodes;
        this.jointConfig = jointConfig;
        this.version = version;
    }
}
