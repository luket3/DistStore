package message;

public class AppendEntriesReply extends Message {
    public int term;
    public String senderId;
    public boolean success;
    public int matchIndex;

    public AppendEntriesReply(int term, String senderId, boolean success, int matchIndex) {
        this.type = "AppendEntriesReply";
        this.term = term;
        this.senderId = senderId;
        this.success = success;
        this.matchIndex = matchIndex;
    }
    
}
