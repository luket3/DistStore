package message;

import cluster.Node;

/**
 * acknowledgement message used to report the outcome of an earlier request.
 *
 * An Ack instance carries a success flag, a human-readable status message,
 * and an optional node reference that can be used to return the updated
 * destination.
 */
public class Ack extends Message {
    /** Indicates whether the referenced operation completed successfully. */
    public boolean success;

    /** Human-readable status text describing the acknowledgement result. */
    public String message;

    /** Optional node reference returned with the acknowledgement. */
    public Node node;

    /**
     * Creates a default acknowledgement with a successful outcome.
     */
    public Ack() {
        super("Ack");
        success = true;
        node = null;
    }

    /**
     * Creates an acknowledgement with a success flag and status text.
     *
     * @param success whether the operation completed successfully
     * @param message human-readable acknowledgement text
     */
    public Ack(boolean success, String message) {
        super("Ack");
        this.success = success;
        this.message = message;
        this.node = null;
    }

    /**
     * Creates an acknowledgement with a success flag, status text, and an
     * optional node reference.
     *
     * @param success whether the operation completed successfully
     * @param message human-readable acknowledgement text
     * @param n node returned as part of the acknowledgement payload
     */
    public Ack(boolean success, String message, Node n) {
        super("Ack");
        this.success = success;
        this.message = message;
        this.node = n;
    }
}
