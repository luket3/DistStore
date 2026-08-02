package raft;

import java.util.ArrayList;
import communication.HandOff;
import communication.Pipe;
import message.Ack;
import message.Message;
import message.RaftConfig;
import message.Reply;

/**
 * Local Raft log container that separates committed and uncommitted entries.
 *
 * The log stores replicated operations in a sequence of LogEntry records, keeps
 * the committed prefix immutable for the local node, and exposes helper methods
 * for append, commit, lookup, range read, and truncation.
 */
public class RaftLog {
    /** Fully committed log entries that are durable for this node's current state. */
    private ArrayList<LogEntry> committedLog;

    /** Log entries accepted by the leader but not yet committed. */
    private ArrayList<LogEntry> uncommittedLog;

    /** Output pipe used to publish committed client or membership messages. */
    private Pipe outPipe;

    /** Shared Raft state associated with this log instance. */
    private RaftState raftState;

    /** Tracks whether a joint configuration update is still waiting to complete. */
    public boolean uncommitedJointConfig;

    /**
     * Creates an empty Raft log attached to the supplied state and output pipe.
     *
     * @param outPipe pipe used to publish committed reply messages
     * @param raftState shared Raft runtime state for the local node
     */
    public RaftLog(Pipe outPipe, RaftState raftState) {
        this.committedLog = new ArrayList<>();
        this.uncommittedLog = new ArrayList<>();
        this.outPipe = outPipe;
        this.raftState = raftState;
        this.uncommitedJointConfig = false;
    }

    /**
     * Appends a new entry to the uncommitted portion of the log.
     *
     * @param msg message payload to store in the log
     * @param term Raft term in which the entry was accepted
     */
    public void appendEntry(Message msg, int term) {
        LogEntry newEntry = new LogEntry(
                msg,
                term,
                committedLog.size() + uncommittedLog.size()
        );
        uncommittedLog.add(newEntry);

        if (msg.type.equals("RaftConfig")) {
            RaftConfig raftConfig = (RaftConfig) msg;
            if (raftConfig.jointConfig)
                this.uncommitedJointConfig = true;
        }
    }

    /**
     * Commits every log entry whose index is less than or equal to the supplied
     * cutoff and publishes any committed client or membership messages.
     *
     * @param upToIndex highest log index to commit from the uncommitted region
     * @return a RaftConfig message when the committed prefix includes a config
     *     change, otherwise null
     */
    public RaftConfig commitEntries(int upToIndex) {
        HandOff.writeToFile("Node " + raftState.id + " " + raftState.level + ": commiting upto index " + upToIndex, raftState.getLogFilePath());

        // pass comitted entries
        RaftConfig rtnValue = null;
        while (!uncommittedLog.isEmpty()
                && uncommittedLog.get(0).index <= upToIndex) {
            LogEntry entryToCommit = uncommittedLog.remove(0);
            committedLog.add(entryToCommit);

            // Publish committed client or membership messages to the output pipe
            if (entryToCommit.msg.type.equals("DictMsg") || entryToCommit.msg.type.equals("NodeMsg")) {
                Reply msg = (Reply) entryToCommit.msg;
                if (this.raftState.type != "leader") {
                    msg.client = null;
                }
                outPipe.put(msg);
            // If the committed entry is a RaftConfig, let the caller know so it can
            // update its local configuration state.
            } else if (entryToCommit.msg.type.equals("RaftConfig")) {
                RaftConfig raftConfig = (RaftConfig) entryToCommit.msg;
                // Let RaftState handle joint vs final config application.
                raftState.proccessNewConfig(raftConfig);
                rtnValue = raftConfig;
                if (raftState.version == raftConfig.version) {
                    raftState.callbackPipe.put(new Ack());
                }
                if (raftConfig.jointConfig)
                    this.uncommitedJointConfig = false;
            }
        }
        return rtnValue;
    }

    /**
     * Returns the message payload stored in the newest committed log entry.
     *
     * @return latest committed message, or null when no committed entries exist
     */
    public Message getLastCommittedCommand() {
        if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).msg;
        }
        return null; // No committed entries
    }

    /**
     * Returns the highest committed log index.
     *
     * @return latest committed index, or -1 when no committed entries exist
     */
    public int getCommitIdx() {
        if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).index;
        }
        return -1; // No committed entries
    }

    /**
     * Returns the highest index currently present in the combined log.
     *
     * @return last available log index, or -1 when the log is empty
     */
    public int getLastIdx() {
        return committedLog.size() + uncommittedLog.size() - 1;
    }

    /**
     * Returns the term associated with the newest entry in the log.
     *
     * @return latest log term, or 0 when the log is empty
     */
    public int getLastTerm() {
        if (!uncommittedLog.isEmpty()) {
            return uncommittedLog.get(uncommittedLog.size() - 1).term;
        } else if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).term;
        }
        return 0; // No entries, so term is 0
    }

    /**
     * Returns the number of entries currently held in the combined log.
     *
     * @return current log size
     */
    public int getSize() {
        return committedLog.size() + uncommittedLog.size();
    }

    /**
     * Returns the log entry at the supplied logical index.
     *
     * @param index zero-based log position to read
     * @return matching LogEntry, or null when the index is outside the current
     *     log range
     */
    public LogEntry get(int index) {
        if (index < committedLog.size()) {
            return committedLog.get(index);
        } else {
            int uncommittedIndex = index - committedLog.size();
            if (uncommittedIndex < uncommittedLog.size()) {
                return uncommittedLog.get(uncommittedIndex);
            }
        }
        return null; // Index out of bounds
    }

    /**
     * Removes uncommitted entries after the supplied cutoff index.
     *
     * @param max highest log index to retain in the uncommitted region
     */
    public void clearTo(int max) {
        if (max == -1 || max < committedLog.size()) {
            clearUncommitted();
            return;
        }
        uncommittedLog.subList(max - committedLog.size() + 1,
                                uncommittedLog.size()).clear();
    }

    /**
     * Returns a contiguous slice of entries from the supplied inclusive range.
     *
     * The range may span both committed and uncommitted regions. If the range
     * is invalid, the method returns null.
     *
     * @param start first index in the requested range
     * @param end last index in the requested range
     * @return list of entries covering the requested range, or null when the
     *     range is outside the valid log bounds
     */
    public ArrayList<LogEntry> get(int start, int end) {
        // Validate
        if (start < 0 || end >= getSize() || end < start)
            return null;

        // Case 1: Entirely in committed log
        ArrayList<LogEntry> result = new ArrayList<>();
        int committedSize = committedLog.size();
        if (end < committedSize) {
            result.addAll(committedLog.subList(start, end + 1));
            return result;
        }

        // Case 2: Entirely in uncommitted log
        if (start >= committedSize) {
            int s = start - committedSize;
            int e = end - committedSize;
            result.addAll(uncommittedLog.subList(s, e + 1));
            return result;
        }

        // Case 3: Spans committed → uncommitted
        result.addAll(committedLog.subList(start, committedSize));
        result.addAll(uncommittedLog.subList(0, end - committedSize + 1));
        return result;
    }

    /**
     * Removes all uncommitted entries from the current log view.
     */
    public void clearUncommitted() {
        uncommittedLog.clear();
    }

    /**
     * Clears both the committed and uncommitted log regions.
     */
    public void wipe() {
        committedLog.clear();
        uncommittedLog.clear();
    }
}