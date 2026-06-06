package message;

import cluster.ConsistentHashMap;

public class Config extends Message {
    public ConsistentHashMap config;

    public Config() {
        super("Config");
        config = null;
    }

    public Config(ConsistentHashMap config) {
        super("Config");
        this.config = config;
    }
}
