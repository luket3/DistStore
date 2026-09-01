package message;

import cluster.Node;
import java.util.Map;

/**
 * Internal topology-update message used to publish cluster membership changes
 * or shard redistribution state through the Raft pipeline.
 */
public class Update extends Message {
    /** Node map representing the current cluster membership snapshot. */
    public Map<String, Node> nodes;

    public boolean shardRemoved;

    /**
     * constructor for child classes to define their type
     *
     * @param type the type of message being constructed
     * @param nodes node map describing the current cluster membership snapshot
     * @param version configuration version attached to this update
     * @param shardRemoved whether the shard has been removed from a Shard
     */
    protected Update(String type, Map<String, Node> nodes, int version, boolean shardRemoved) {
        super(type, version);
        this.nodes = nodes;
        this.shardRemoved = shardRemoved;
    }

    /**
     * Creates a membership-only update message.
     *
     * @param nodes node map describing the current cluster membership snapshot
     * @param version configuration version attached to this update
     * @param shardRemoved whether the shard has been removed from a Shard
     */
    public Update(Map<String, Node> nodes, int version, boolean shardRemoved) {
        this("Update", nodes, version, shardRemoved);
    }
}

