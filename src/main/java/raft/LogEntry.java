package raft;

import message.Message;

/*
 * File: log_entry.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Represents a single entry in the Raft log.
 */

/**
 * Represents a single entry in the Raft log.
 *
 * <p>Each entry contains the original command string, the leader term when the
 * entry was created, and the log index.</p>
 */
public class LogEntry {
    /** The command associated with this log entry. */
    final Message msg;

    /** Raft term when this entry was created. */
    final int term;

    /** Log index for this entry. */
    final int index;

    /**
     * Create a new log entry.
     *
     * @param command the command string
     * @param term the term number
     * @param index the index in the log
     */
    LogEntry(Message msg, int term, int index) {
        this.msg =  msg;
        this.term = term;
        this.index = index;
    }
}
