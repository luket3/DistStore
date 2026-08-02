package message;

import cluster.Node;
import java.util.Map;
import cluster.ConsistentHashMap;

/**
 * Internal topology-update message used to publish cluster membership changes
 * or shard redistribution state through the Raft pipeline.
 *
 * The action field indicates the intent of the update, while the payload can
 * carry either a simple node map or a fuller cluster snapshot for more
 * advanced distribution workflows.
 */
public class Update extends Message {
    /** Node map representing the current cluster membership snapshot. */
    public Map<String, Node> nodes;

    /** Operation type for the update message. */
    public String action;

    /** Optional full cluster map carried for richer redistribution workflows. */
    public ConsistentHashMap cluster;

    /** Configuration version associated with the update. */
    public int version;

    /**
     * Creates a membership-only update message.
     *
     * @param action operation type associated with the update
     * @param nodes node map describing the current cluster membership snapshot
     * @param version configuration version attached to this update
     */
    public Update(String action, Map<String, Node> nodes, int version) {
        super("Update");
        this.nodes = nodes;
        this.action = action;
        this.cluster = null;
        this.version = version;
    }

    /**
     * Creates a distribution update message that carries both node metadata and
     * the current consistent-hash topology snapshot.
     *
     * @param action operation type associated with the update
     * @param nodes node map describing the membership subset being published
     * @param cluster full cluster topology snapshot for redistribution handling
     * @param version configuration version attached to this update
     */
    public Update(String action, Map<String,Node> nodes, ConsistentHashMap cluster, int version) {
        super("Distribute");
        this.nodes = nodes;
        this.action = action;
        this.cluster = cluster;
        this.version = version;
    }
}

