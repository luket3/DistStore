package message;

import cluster.Node;
import java.util.Map;
import cluster.ConsistentHashMap;

public class Update extends Message {
    public Map<String, Node> nodes;
    public String action;
    public ConsistentHashMap cluster;
    public int version;

    public Update(String action, Map<String, Node> nodes, int version) {
        super("Update");
        this.nodes = nodes;
        this.action = action;
        this.cluster = null;
        this.version = version;
    }

    // action should be "Distribute"
    public Update(String action, Map<String,Node> nodes, ConsistentHashMap cluster, int version) {
        super("Update");
        this.nodes = nodes;
        this.action = action;
        this.cluster = cluster;
        this.version = version;
    }
}

