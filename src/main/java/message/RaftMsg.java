package message;

public class RaftMsg extends Message {

    public String level;

    public RaftMsg(String type, String level, int version) {
        super(type, version);
        this.level = level;
    }
}
