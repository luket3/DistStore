package message;

/**
 * Represents a RequestVote RPC request in the Raft consensus algorithm.
 * Sent by candidates to gather votes during leader election.
 */
public class RequestVote extends RaftMsg {
    /** The term of the candidate requesting votes. */
    public int term;
    /** The candidate requesting votes. */
    public String candidateId;
    /** Index of the candidate's last log entry (for log consistency check). */
    public int lastLogIndex;
    /** Term of the candidate's last log entry (for log consistency check). */
    public int lastLogTerm;

    /**
     * Constructs a new RequestVote RPC.
     *
     * @param level the Raft cluster level ("Shard" or "Cluster")
     * @param term the candidate's current term
     * @param candidateId the candidate requesting votes
     * @param lastLogIndex index of the candidate's last log entry
     * @param lastLogTerm term of the candidate's last log entry
     */
    public RequestVote(String level, int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        super("RequestVote", level);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
