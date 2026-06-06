package message;

import java.util.ArrayList;
import raft.LogEntry;

public class AppendEntries extends Message {
    public int term;
    public String leaderId;
    public int prevLogIndex;
    public int prevLogTerm;
    public int leaderCommit;
    public ArrayList<LogEntry> entries;

    public AppendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, ArrayList<LogEntry> entries) {
        this.type = "AppendEntries";
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }
    
}
