package message;

/**
 * Represents an AppendEntries RPC response in the Raft consensus algorithm.
 * Sent by followers in response to AppendEntries requests from leaders.
 */
public class AppendEntriesReply extends RaftMsg {
    /** The term of the responder (follower). */
    public int term;
    /** The identifier of the node sending this response. */
    public String senderId;
    /** True if the follower contained the matching entry at prevLogIndex and prevLogTerm. */
    public boolean success;
    /** The index of the highest log entry known to be replicated on the follower. */
    public int matchIndex;

    /**
     * Constructs a new AppendEntriesReply.
     *
     * @param level the Raft cluster level ("Shard" or "Cluster")
     * @param term the current term of the responder
     * @param senderId the identifier of the node sending this response
     * @param success true if the follower accepted the append entries request
     * @param matchIndex the highest log entry index known to be replicated on the follower
     */
    public AppendEntriesReply(String level, int term, String senderId, boolean success, int matchIndex) {
        super("AppendEntriesReply", level);
        this.term = term;
        this.senderId = senderId;
        this.success = success;
        this.matchIndex = matchIndex;
    }

}
