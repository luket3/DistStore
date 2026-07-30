package message;

public class Response extends Message {
    public String msg;

    public Response(String message, int version) {
        super("Response", version);
        this.msg = message;
    }
}
