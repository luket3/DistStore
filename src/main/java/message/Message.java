package message;

/**
 * Base type for all wire-format messages exchanged between the cluster nodes.
 *
 * The type discriminator is used by the Gson deserializer to select
 * the concrete message subtype when a request or response is received.
 */
public class Message {
    /** Discriminator used by the message deserializer and protocol handlers. */
    public String type;

    /** cluster version the given message belongs too */
    public int version;

    /**
     * Creates a message with the supplied protocol discriminator.
     *
     * @param type concrete message type identifier
     * @param version the cluster version the given message belongs too
     */
    protected Message(String type, int version) {
        this.type = type;
        this.version = version;
    }

    /**
     * Creates a default message instance.
     */
    public Message() {
        this.type = "Message";
    }
}
