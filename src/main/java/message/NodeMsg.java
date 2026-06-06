package message;

public class NodeMsg extends Message {
    public String action;
    public String nodeId;

    public NodeMsg(String action, String nodeId) {
        super("NodeMsg");
        this.action = action;
        this.nodeId = nodeId;
    }
    
}
