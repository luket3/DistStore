package message;

import cluster.Node;

/**
 * Client-facing key-value operation carried through the Raft pipeline.
 *
 * <p>Supported actions are {@code Get}, {@code Put}, and {@code Delete}.
 * The message also carries a client reference so the receiving node can send
 * the response back to the originator.</p>
 */
public class DictMsg extends Reply {
    /**
     * Operation to apply to the distributed key-value store.
     */
    public String action;

    /**
     * Key to read, write, or remove.
     */
    public String key;

    /**
     * Value for {@code Put} operations; otherwise {@code null}.
     */
    public String value;

    /**
     * Creates a dictionary message without an explicit reply destination.
     *
     * @param action the operation type
     * @param key the target key
     * @param value the value for write operations
     * @param version configuration version carried by the request
     */
    public DictMsg(String action, String key, String value, int version) {
        super("DictMsg", version);
        this.action = action;
        this.key = key;
        this.value = value;

    }

    /**
     * Creates a dictionary message that can return a reply directly to the
     * supplied client node.
     *
     * @param action the operation type
     * @param key the target key
     * @param value the value for write operations
     * @param version configuration version carried by the request
     * @param client the client node that should receive the response
     */
    public DictMsg(String action, String key, String value, int version, Node client) {
        super("DictMsg", version, client);
        this.action = action;
        this.key = key;
        this.value = value;
    }
}
