package message;

public class Response extends Message {
    public String msg;

    public Response(String message) {
        super("Response");
        this.msg = message;
    }
}
