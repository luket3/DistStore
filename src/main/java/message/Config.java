package message;

import cluster.ConsistentHashMap;
import cluster.Node;

/**
 * Configuration snapshot reply used by clients to install the latest cluster
 * view locally.
 */
public class Config extends Reply {
    /**
     * Serialized cluster topology snapshot returned to the requester.
     */
    public ConsistentHashMap config;

    /**
     * Creates an empty configuration reply.
     */
    public Config() {
        super("Config", -1);
        config = null;
    }

    /**
     * Creates a configuration reply addressed to a specific client.
     *
     * @param client reply destination
     */
    public Config(Node client) {
        super("Config", -1, client);
        config = null;  
    }

    /**
     * Creates a configuration reply containing the current cluster map.
     *
     * @param config current cluster topology snapshot
     * @param version configuration version associated with the snapshot
     */
    public Config(ConsistentHashMap config, int version) {
        super("Config", version);
        this.config = config;
    }
}
