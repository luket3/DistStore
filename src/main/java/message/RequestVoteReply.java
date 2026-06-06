package message;

public class RequestVoteReply extends Message {
    public int term;
    public String senderId;
    public boolean voteGranted;

    public RequestVoteReply(int term, String senderId, boolean voteGranted) {
        this.type = "RequestVoteReply";
        this.term = term;
        this.senderId = senderId;
        this.voteGranted = voteGranted;
    }
    
}
