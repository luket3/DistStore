package raft;

import message.Message;

/**
 * Immutable record stored in the local Raft log.
 *
 * Each entry holds the message payload that should be replayed, the Raft term
 * in which the leader accepted that command, and the log index assigned to
 * the entry within the replicated sequence.
 */
public class LogEntry {
    /* The message payload stored in this log position. */
    public final Message msg;

    /** Raft term in which the entry was created. */
    public final int term;

    /** The log index assigned to this entry. */
    public final int index;

    /**
     * Creates a new log entry.
     *
     * @param msg message payload stored in the log position
     * @param term Raft term in which the entry was accepted
     * @param index log index assigned to the entry
     */
    LogEntry(Message msg, int term, int index) {
        this.msg =  msg;
        this.term = term;
        this.index = index;
    }
}
