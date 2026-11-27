package message;

import cluster.Node;
import java.util.Map;
import java.util.Set;

/**
 * Carries a cluster-node configuration through the Raft message pipeline.
 * This message is used to replicate cluster configuration changes during
 * joint consensus transitions in the Raft consensus algorithm.
 *
 * The new and old configurations usually share most of their nodes, so each
 * node's address is stored once in the message's node list and the two
 * configurations hold only node ids.
 */
public class RaftConfig extends Message {
    /** Ids of the nodes that make up the configuration being replicated. */
    private Set<String> nodeIds;

    /** Ids of the previous configuration for joint consensus, or null. */
    private Set<String> oldNodeIds;

    /** Whether this is a joint-config transition. */
    public boolean jointConfig;

    /**
     * used for initalising child classes with their type
     *
     * @param type the type of message being created
     * @param nodes the node configuration to replicate
     * @param oldNodes the previous configuration for joint consensus (may be null for initial configuration)
     * @param jointConfig whether this represents a joint-config transition
     * @param version the raft message version
     */
    protected RaftConfig(String type, Map<String, Node> nodes, Map<String, Node> oldNodes, boolean jointConfig, int version) {
        super(type, version);
        this.nodeIds = ref(nodes);
        this.oldNodeIds = ref(oldNodes);
        this.jointConfig = jointConfig;
    }

    /**
     * Nodes that make up the configuration being replicated.
     *
     * @return a new map of the nodes keyed by id, or null if none were set
     */
    public Map<String, Node> nodes() {
        return deref(nodeIds);
    }

    /**
     * Previous configuration for joint consensus.
     *
     * @return a new map of the nodes keyed by id, or null for a final configuration
     */
    public Map<String, Node> oldNodes() {
        return deref(oldNodeIds);
    }

    /**
     * Constructs a new Raft configuration message.
     *
     * @param nodes the node configuration to replicate
     * @param oldNodes the previous configuration for joint consensus (may be null for initial configuration)
     * @param jointConfig whether this represents a joint-config transition
     * @param version the raft message version
     */
    public RaftConfig(Map<String, Node> nodes, Map<String, Node> oldNodes, boolean jointConfig, int version) {
        this("RaftConfig", nodes, oldNodes, jointConfig, version);
    }
}
