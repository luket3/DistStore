package message;

import cluster.Node;

public class NodeMsg extends Message {
    public String action;
    public Node node;

    public NodeMsg(String action, String nodeID) {
        super("NodeMsg");
        this.action = action;
        node = new Node(nodeID, null, -1);
    }

    public NodeMsg(String action, Node node) {
        super("NodeMsg");
        this.action = action;
        this.node = node;
    } 
}
