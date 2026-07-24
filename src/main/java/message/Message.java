package message;

public class Message {
    public String type;
    public int version;

    protected Message(String type, int version) {
        this.type = type;
        this.version = version;
    }

    public Message() {
        this.type = "Message";
        this.version = -1;
    }
}
