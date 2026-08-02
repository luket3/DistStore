package message;

/**
 * Base type for all wire-format messages exchanged between the cluster nodes.
 *
 * The {@code type} discriminator is used by the Gson deserializer to select
 * the concrete message subtype when a request or response is received.
 */
public class Message {
    /**
     * Discriminator used by the message deserializer and protocol handlers.
     */
    public String type;

    /**
     * Creates a message with the supplied protocol discriminator.
     *
     * @param type concrete message type identifier
     */
    protected Message(String type) {
        this.type = type;
    }

    /**
     * Creates a default message instance.
     */
    public Message() {
        this.type = "Message";
    }
}
