package message;

public class Message {
    public String type;

    protected Message(String type) {
        this.type = type;
    }

    public Message() {
        this.type = "Message";
    }
}
