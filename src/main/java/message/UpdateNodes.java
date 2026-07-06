package message;

import cluster.Node;
import java.util.Map;

public class UpdateNodes extends Message {
    public Map<String, Node> nodes;
    public String action;
    public int version;

    public UpdateNodes(Map<String, Node> nodes, String action, int version) {
        super("UpdateNodes");
        this.nodes = nodes;
        this.action = action;
        this.version = version;
    }
    
}
