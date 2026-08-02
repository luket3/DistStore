package message;

import cluster.Node;

/**
 * Cluster membership control message used to add or remove nodes from the
 * network view.
 */
public class NodeMsg extends Reply {
    /**
     * Membership operation, such as {@code Add} or {@code Remove}.
     */
    public String action;

    /**
     * Target node reference involved in the membership change.
     */
    public Node node;

    /**
     * Creates a node membership message using only a node identifier.
     *
     * @param action the membership action
     * @param nodeID identifier of the node being added or removed
     * @param version configuration version carried by the request
     */
    public NodeMsg(String action, String nodeID, int version) {
        super("NodeMsg", version);
        this.action = action;
        node = new Node(nodeID, null, -1);
    }

    /**
     * Creates a node membership message for a fully qualified node.
     *
     * @param action the membership action
     * @param node the node to add or remove
     * @param version configuration version carried by the request
     */
    public NodeMsg(String action, Node node, int version) {
        super("NodeMsg", version);
        this.action = action;
        this.node = node;
    }

    /**
     * Creates a node membership message that can be answered directly to the
     * supplied client node.
     *
     * @param action the membership action
     * @param node the node to add or remove
     * @param version configuration version carried by the request
     * @param client the client node that should receive the result
     */
    public NodeMsg(String action, Node node, int version, Node client) {
        super("NodeMsg", version, client);
        this.action = action;
        this.node = node;
    }
}
