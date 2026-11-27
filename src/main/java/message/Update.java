package message;

import cluster.Node;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Internal topology-update message used to publish cluster membership changes
 * or shard redistribution state through the Raft pipeline.
 *
 * Node addresses are stored once in the message's node list; the membership
 * and the per-shard member lists hold only node ids.
 */
public class Update extends Message {
    /** Ids of the nodes in the current membership snapshot. */
    private Set<String> nodeIds;

    public boolean shardRemoved;

    /**
     * For a shard removal: the ring after the removal (position to shard id),
     * used to send the removed shard's keys to their new owners. Null otherwise.
     */
    public TreeMap<Long, String> ring;

    /** For a shard removal: the member ids of every shard after the removal. Null otherwise. */
    private Map<String, Set<String>> shardMemberIds;

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
        this.nodeIds = ref(nodes);
        this.shardRemoved = shardRemoved;
    }

    /**
     * Nodes in the current membership snapshot.
     *
     * @return a new map of the nodes keyed by id, or null if none were set
     */
    public Map<String, Node> nodes() {
        return deref(nodeIds);
    }

    /**
     * For a shard removal: the members of every shard after the removal.
     *
     * @return shard id to that shard's nodes, or null if this is not a shard removal
     */
    public Map<String, Map<String, Node>> shardMembers() {
        if (shardMemberIds == null)
            return null;
        Map<String, Map<String, Node>> members = new HashMap<>();
        for (Map.Entry<String, Set<String>> e : shardMemberIds.entrySet())
            members.put(e.getKey(), deref(e.getValue()));
        return members;
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
        if (shardMembers != null) {
            this.shardMemberIds = new HashMap<>();
            for (Map.Entry<String, Map<String, Node>> e : shardMembers.entrySet())
                this.shardMemberIds.put(e.getKey(), ref(e.getValue()));
        }
    }
}

