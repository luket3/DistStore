package message;

/**
 * Represents a RequestVote RPC response in the Raft consensus algorithm.
 * Sent by voters in response to RequestVote requests from candidates.
 */
public class RequestVoteReply extends RaftMsg {
    /** The term of the responding node. */
    public int term;
    
    /** True if the voter granted its vote to the candidate. */
    public boolean voteGranted;

    /**
     * Constructs a new RequestVoteReply RPC response.
     *
     * @param level the Raft cluster level ("Shard" or "Cluster")
     * @param term the current term of the responding node
     * @param senderId the ID of the node sending this response
     * @param voteGranted true if the vote was granted to the candidate
     * @param version the version of the cluster configuration that this message belongs too
     */
    public RequestVoteReply(String level, int term, String senderId, boolean voteGranted, int version) {
        super("RequestVoteReply", level, senderId, version);
        this.term = term;
        this.voteGranted = voteGranted;
    }

}
