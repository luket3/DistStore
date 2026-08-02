package message;

import cluster.Node;
import java.util.Map;
import cluster.ConsistentHashMap;

/**
 * Internal topology-update message used to push changed node membership or
 * shard topology information through the Raft pipeline.
 *
 * The {@code action} field is intended to distinguish update styles such as
 * a plain membership refresh or a shard redistribution event.
 */
public class Update extends Message {
    /**
     * Node map representing the current cluster membership snapshot.
     */
    public Map<String, Node> nodes;

    /**
     * Operation type for the update message.
     */
    public String action;

    /**
     * Optional full cluster map carried for richer redistribution workflows.
     */
    public ConsistentHashMap cluster;

    /**
     * Configuration version associated with the update.
     */
    public int version;

    /**
     * Creates a membership-only update message.
     *
     * @param action the update action name
     * @param nodes the node map to publish
     * @param version configuration version for the update
     */
    public Update(String action, Map<String, Node> nodes, int version) {
        super("Update");
        this.nodes = nodes;
        this.action = action;
        this.cluster = null;
        this.version = version;
    }

    /**
     * Creates a more complete update message that carries both node metadata
     * and the current consistent-hash topology snapshot.
     *
     * @param action the update action name
     * @param nodes the node map to publish
     * @param cluster a full cluster topology snapshot
     * @param version configuration version for the update
     */
    public Update(String action, Map<String,Node> nodes, ConsistentHashMap cluster, int version) {
        super("Update");
        this.nodes = nodes;
        this.action = action;
        this.cluster = cluster;
        this.version = version;
    }
}

