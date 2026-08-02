package message;

import cluster.Node;

/**
 * Common base class for request and response messages that carry a client
 * reply destination and a cluster configuration version.
 */
public class Reply extends Message {
    /**
     * Client node that should receive the response for this message.
     */
    public Node client;

    /**
     * Logical cluster configuration version associated with this message.
     */
    public int version;

    /**
     * Creates a reply-capable message with an explicit client response target.
     *
     * @param type concrete message type discriminator
     * @param version configuration version associated with the operation
     * @param client receiving client node
     */
    public Reply(String type, int version, Node client) {
        super(type);
        this.client = client;
        this.version = version;
    }

    /**
     * Creates a reply-capable message that does not target a specific client.
     *
     * @param type concrete message type discriminator
     * @param version configuration version associated with the operation
     */
    public Reply(String type, int version) {
        super(type);
        this.client = null;
        this.version = version;
    }
    
}
