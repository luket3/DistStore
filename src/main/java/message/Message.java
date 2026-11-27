package message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import cluster.Node;

/**
 * Base type for all wire-format messages exchanged between the cluster nodes.
 *
 * The type discriminator is used by the Gson deserializer to select
 * the concrete message subtype when a request or response is received.
 */
public class Message {
    /** Discriminator used by the message deserializer and protocol handlers. */
    public String type;

    /** cluster version the given message belongs too */
    public int version;

    /**
     * Every node this message refers to, each listed once. Messages that
     * carry several overlapping groups of nodes (e.g. old and new membership)
     * store each group as a set of ids into this list, rather than repeating
     * the full node addresses in every group. Null when the message carries
     * no node groups, so it is left out of the JSON.
     */
    private List<Node> allNodes;

    /**
     * Creates a message with the supplied protocol discriminator.
     *
     * @param type concrete message type identifier
     * @param version the cluster version the given message belongs too
     */
    protected Message(String type, int version) {
        this.type = type;
        this.version = version;
    }

    /**
     * Creates a default message instance.
     */
    public Message() {
        this.type = "Message";
    }

    /**
     * Adds a group of nodes to this message's node list and returns the ids
     * the message should store for that group.
     *
     * @param group nodes keyed by id, or null
     * @return the group's ids in iteration order, or null if the group is null
     */
    protected Set<String> ref(Map<String, Node> group) {
        if (group == null)
            return null;
        if (allNodes == null)
            allNodes = new ArrayList<>();
        Set<String> ids = new LinkedHashSet<>();
        for (Node n : group.values()) {
            if (n == null || n.id == null)
                continue;
            ids.add(n.id);
            if (find(n.id) == null)
                allNodes.add(n);
        }
        return ids;
    }

    /**
     * Resolves a group of node ids stored by {@link #ref} back into nodes.
     *
     * @param ids node ids, or null
     * @return a new map of the matching nodes keyed by id, or null if ids is null
     */
    protected Map<String, Node> deref(Collection<String> ids) {
        if (ids == null)
            return null;
        Map<String, Node> group = new HashMap<>();
        for (String id : ids) {
            Node n = find(id);
            if (n != null)
                group.put(id, n);
        }
        return group;
    }

    /**
     * Looks up a node in this message's node list.
     *
     * @param id node id
     * @return the node, or null if this message does not list it
     */
    private Node find(String id) {
        if (allNodes == null)
            return null;
        for (Node n : allNodes)
            if (n.id.equals(id))
                return n;
        return null;
    }
}
