package message;

/**
 * Represents a RequestVote RPC request in the Raft consensus algorithm.
 * Sent by candidates to gather votes during leader election.
 */
public class RequestVote extends RaftMsg {
    /** The term of the candidate requesting votes. */
    public int term;

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
     * @param version the version of the cluster configuration that this message belongs too
     */
    public RequestVote(String level, int term, String candidateId, int lastLogIndex, int lastLogTerm, int version) {
        super("RequestVote", level, candidateId, version);
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
