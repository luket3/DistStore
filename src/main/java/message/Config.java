package message;

import cluster.ConsistentHashMap;

public class Config extends Message {
    public ConsistentHashMap config;

    public Config() {
        super("Config", -1);
        config = null;
    }

    public Config(ConsistentHashMap config, int version) {
        super("Config", version);
        this.config = config;
    }
}
