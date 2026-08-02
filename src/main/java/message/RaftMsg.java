package message;

/**
 * Base message type for Raft protocol traffic.
 *
 * A Raft message extends the generic wire-format message with a routing level
 * discriminator so the receiving node can decide whether the payload belongs
 * to the shard Raft pipeline or the cluster Raft pipeline.
 */
public class RaftMsg extends Message {

    /**
     * Pipeline classification for the message.
     *
     * Typical values are Shard or Cluster, which determine where the message
     * is dispatched for processing.
     */
    public String level;

    /**
     * Creates a Raft message with a concrete message type and routing level.
     *
     * @param type concrete Raft message discriminator such as AppendEntries or
     *     RequestVote
     * @param level pipeline routing label used by the server dispatcher
     */
    public RaftMsg(String type, String level) {
        super(type);
        this.level = level;
    }
}
