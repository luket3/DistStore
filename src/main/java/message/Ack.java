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

    public Ack() {
        super("Ack");
        success = true;
        node = null;
    }

    public Ack(boolean success, String message) {
        super("Ack");
        this.success = success;
        this.message = message;
        this.node = null;
    }

    public Ack(boolean success, String message, Node n) {
        super("Ack");
        this.success = success;
        this.message = message;
        this.node = n;
    }
}
