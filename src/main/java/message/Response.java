package message;

/**
 * Simple response message used to communicate operation results back to clients.
 */
public class Response extends Message {
    /** The response message content. */
    public String msg;

    /**
     * Constructs a new Response message.
     *
     * @param message the response content to send to the client
     */
    public Response(String message) {
        super("Response");
        this.msg = message;
    }
}
