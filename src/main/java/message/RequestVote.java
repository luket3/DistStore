package message;

public class RequestVote extends RaftMsg {
    public int term;
    public String candidateId;
    public int lastLogIndex;
    public int lastLogTerm;

    public RequestVote(String level, int term, String candidateId, int lastLogIndex, int lastLogTerm, int version) {
        super("RequestVote", level, version);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
