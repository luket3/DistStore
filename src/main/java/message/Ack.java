package message;

import cluster.Node;

/**
 * Lightweight acknowledgement message used to signal that a previously
 * requested operation has been committed and may safely proceed.
 */
public class Ack extends Message {
    public boolean success;
    public String message;
    public Node node;

    public Ack(int version) {
        super("Ack", version);
        success = true;
        node = null;
    }

    public Ack(int version, boolean success, String message) {
        super("Ack", version);
        this.success = success;
        this.message = message;
        this.node = null;
    }

        public Ack(boolean success, String message, Node n) {
        super("Ack", -1);
        this.success = success;
        this.message = message;
        this.node = n;
    }
}
