package communication;

public class MessageInfo {
    public String message;
    public Comm comm;

    public MessageInfo(String message, Comm comm) {
        this.message = message;
        this.comm = comm;
    }

    public MessageInfo(String message) {
        this.message = message;
        this.comm = null;
    }
}
