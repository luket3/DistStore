package message;

import cluster.Node;
import java.util.Map;
import cluster.ConsistentHashMap;

public class UpdateShard extends Message {
    public Map<String, Node> nodes;
    public String action;
    public ConsistentHashMap cluster;

    // action should be "Update"
    public UpdateShard(String action, Map<String, Node> nodes, int version) {
        super("UpdateShard", version);
        this.nodes = nodes;
        this.action = action;
        this.cluster = null;
    }

    // action should be "Distribute"
    public UpdateShard(String action, Map<String,Node> nodes, ConsistentHashMap cluster, int version) {
        super("UpdateShard", version);
        this.nodes = nodes;
        this.action = action;
        this.cluster = cluster;
    }
    
}
