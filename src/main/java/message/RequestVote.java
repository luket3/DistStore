package message;

public class RequestVote extends Message {
    public int term;
    public String candidateId;
    public int lastLogIndex;
    public int lastLogTerm;

    public RequestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        super("RequestVote");
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
    
}
