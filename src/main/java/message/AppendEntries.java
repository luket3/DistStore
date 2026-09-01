package message;

import java.util.ArrayList;
import raft.LogEntry;

/**
 * Raft AppendEntries RPC sent by the leader to replicate log entries or
 * maintain leadership heartbeat with followers.
 *
 * The message carries the leader's current term, the identity of the leader,
 * the log index and term immediately preceding the new entries, the commit
 * point the leader has applied, and the batch of log entries to append.
 */
public class AppendEntries extends RaftMsg {
    /** The current Raft term of the leader sending this RPC. */
    public int term;

    /** Index of the log entry immediately before the new batch. */
    public int prevLogIndex;

    /** Term of the log entry immediately before the new batch. */
    public int prevLogTerm;

    /** Latest log index the leader has committed. */
    public int leaderCommit;

    /** Entries to append to the follower's log, or an empty list for heartbeat. */
    public ArrayList<LogEntry> entries;

    /**
     * Creates a new AppendEntries RPC for the given Raft level.
     *
     * @param level raft pipeline level for the message, such as Shard or Cluster
     * @param term leader term carried by the RPC
     * @param leaderId identifier of the leader sending the request
     * @param prevLogIndex log index immediately preceding the new entries
     * @param prevLogTerm term of the preceding log entry
     * @param leaderCommit latest committed log index known to the leader
     * @param entries list of log entries to append, or an empty list for heartbeat
     * @param version the version of the cluster configuration that this message belongs too
     */
    public AppendEntries(String level, int term, String leaderId, int prevLogIndex, 
                         int prevLogTerm, int leaderCommit, ArrayList<LogEntry> entries, int version) {
        super("AppendEntries", level, leaderId, version);
        this.term = term;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }
    
}
