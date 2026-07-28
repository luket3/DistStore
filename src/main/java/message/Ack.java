package message;

/**
 * Lightweight acknowledgement message used to signal that a previously
 * requested operation has been committed and may safely proceed.
 */
public class Ack extends Message {

    public Ack(int version) {
        super("Ack", version);
    }
}
