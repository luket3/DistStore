package message;

import cluster.Node;

public class NodeMsg extends Reply {
    public String action;
    public Node node;

    public NodeMsg(String action, String nodeID, int version) {
        super("NodeMsg", version);
        this.action = action;
        node = new Node(nodeID, null, -1);
    }

    public NodeMsg(String action, Node node, int version) {
        super("NodeMsg", version);
        this.action = action;
        this.node = node;
    }

    public NodeMsg(String action, Node node, int version, Node client) {
        super("NodeMsg", version, client);
        this.action = action;
        this.node = node;
    }
}
