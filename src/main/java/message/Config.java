package message;

import cluster.ConsistentHashMap;
import cluster.Node;

public class Config extends Reply {
    public ConsistentHashMap config;

    public Config() {
        super("Config", -1);
        config = null;
    }

    public Config(Node client) {
        super("Config", -1, client);
        config = null;  
    }

    public Config(ConsistentHashMap config, int version) {
        super("Config", version);
        this.config = config;
    }
}
