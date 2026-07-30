package message;

public class RequestVoteReply extends RaftMsg {
    public int term;
    public String senderId;
    public boolean voteGranted;

    public RequestVoteReply(String level, int term, String senderId, boolean voteGranted) {
        super("RequestVoteReply", level);
        this.term = term;
        this.senderId = senderId;
        this.voteGranted = voteGranted;
    }
    
}
