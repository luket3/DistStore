package message;

import java.util.ArrayList;
import raft.LogEntry;

/**
 * Represents an AppendEntries RPC request in the Raft consensus algorithm.
 * Used by the leader to replicate log entries and provide heartbeat to followers.
 */
public class AppendEntries extends RaftMsg {
    public int term;
    public String leaderId;
    public int prevLogIndex;
    public int prevLogTerm;
    public int leaderCommit;
    public ArrayList<LogEntry> entries;

    public AppendEntries(String level, int term, String leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, ArrayList<LogEntry> entries) {
        super("AppendEntries", level);
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }
    
}
