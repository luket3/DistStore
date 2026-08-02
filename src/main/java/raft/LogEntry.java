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
 * Immutable record stored in the local Raft log.
 *
 * The entry contains the serialized operation to apply, the term in which
 * the leader accepted the entry, and the relative index assigned to it in the
 * replicated log.
 */
public class LogEntry {
    /**
     * The message payload stored in this log position.
     */
    final Message msg;

    /**
     * Raft term in which the entry was created.
     */
    final int term;

    /**
     * The log index assigned to this entry.
     */
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
