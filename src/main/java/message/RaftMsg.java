package message;

public class RaftMsg extends Message {

    public String level;

    public RaftMsg(String type, String level) {
        super(type);
        this.level = level;
    }
}
