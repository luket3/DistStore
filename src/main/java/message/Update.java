package message;

import cluster.Node;
import java.util.Map;
import java.util.TreeMap;

/**
 * Internal topology-update message used to publish cluster membership changes
 * or shard redistribution state through the Raft pipeline.
 */
public class Update extends Message {
    /** Node map representing the current cluster membership snapshot. */
    public Map<String, Node> nodes;

    public boolean shardRemoved;

    /**
     * For a shard removal: the ring after the removal (position to shard id),
     * used to send the removed shard's keys to their new owners. Null otherwise.
     */
    public TreeMap<Long, String> ring;

    /** For a shard removal: the members of every shard after the removal. Null otherwise. */
    public Map<String, Map<String, Node>> shardMembers;

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

    /**
     * Creates a shard removal update that carries what the removed shard's
     * nodes need to hand their keys over to the new owners.
     *
     * @param nodes nodes of the shard the local node now belongs to
     * @param version configuration version attached to this update
     * @param ring ring after the removal, as ring position to shard id
     * @param shardMembers members of every shard after the removal
     */
    public Update(Map<String, Node> nodes, int version, TreeMap<Long, String> ring,
                  Map<String, Map<String, Node>> shardMembers) {
        this("Update", nodes, version, true);
        this.ring = ring;
        this.shardMembers = shardMembers;
    }
}

