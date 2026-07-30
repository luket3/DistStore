package message;

public class AppendEntriesReply extends RaftMsg {
    public int term;
    public String senderId;
    public boolean success;
    public int matchIndex;

    public AppendEntriesReply(String level, int term, String senderId, boolean success, int matchIndex) {
        super("AppendEntriesReply", level);
        this.term = term;
        this.senderId = senderId;
        this.success = success;
        this.matchIndex = matchIndex;
    }
    
}
