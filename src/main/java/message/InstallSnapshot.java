package message;

/**
 * Raft InstallSnapshot RPC sent by a leader to a follower whose next required
 * log entry has already been compacted into the leader's snapshot.
 *
 * The whole snapshot is sent in a single message. The follower answers with
 * an {@link AppendEntriesReply} whose matchIndex is the snapshot's
 * lastIncludedIndex, so the leader can reuse its normal replication handling.
 */
public class InstallSnapshot extends RaftMsg {
    /** Current Raft term of the leader sending this RPC. */
    public int term;

    /** Snapshot that the follower should install. */
    public Snapshot snapshot;

    /**
     * Creates a new InstallSnapshot RPC.
     *
     * @param level raft pipeline level for the message, such as Shard or Cluster
     * @param term leader term carried by the RPC
     * @param leaderId identifier of the leader sending the request
     * @param snapshot snapshot to install on the follower
     * @param version the version of the cluster configuration that this message belongs too
     */
    public InstallSnapshot(String level, int term, String leaderId, Snapshot snapshot, int version) {
        super("InstallSnapshot", level, leaderId, version);
        this.term = term;
        this.snapshot = snapshot;
    }
}
