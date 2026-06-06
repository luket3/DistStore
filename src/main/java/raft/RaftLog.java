package raft;

import java.util.ArrayList;

import communication.Pipe;

import message.DictMsg;

public class RaftLog {
    private ArrayList<LogEntry> committedLog;
    private ArrayList<LogEntry> uncommittedLog;
    private Pipe stateMachineIn;

    public RaftLog(Pipe stateMachineIn) {
        this.committedLog = new ArrayList<>();
        this.uncommittedLog = new ArrayList<>();
        this.stateMachineIn = stateMachineIn;
    }

    public void appendEntry(DictMsg command, int term) {
        LogEntry newEntry = new LogEntry(
                command,
                term,
                committedLog.size() + uncommittedLog.size()
        );
        uncommittedLog.add(newEntry);
    }

    public void commitEntries(int upToIndex) {
        while (!uncommittedLog.isEmpty()
                && uncommittedLog.get(0).index <= upToIndex) {
            LogEntry entryToCommit = uncommittedLog.remove(0);
            committedLog.add(entryToCommit);
            stateMachineIn.put(entryToCommit.command);
        }
    }

    public DictMsg getLastCommittedCommand() {
        if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).command;
        }
        return null; // No committed entries
    }

    public int getCommitIdx() {
        if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).index;
        }
        return -1; // No committed entries
    }

    public int getLastIdx() {
        return committedLog.size() + uncommittedLog.size() - 1;
    }

    public int getLastTerm() {
        if (!uncommittedLog.isEmpty()) {
            return uncommittedLog.get(uncommittedLog.size() - 1).term;
        } else if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).term;
        }
        return 0; // No entries, so term is 0
    }

    public int getSize() {
        return committedLog.size() + uncommittedLog.size();
    }

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

    public void clearTo(int max) {
        if (max == -1 || max < committedLog.size()) {
            clearUncommitted();
            return;
        }

        uncommittedLog.subList(max - committedLog.size() + 1,
                                uncommittedLog.size()).clear();
    }

    public ArrayList<LogEntry> get(int start, int end) {
        // Validate
        if (start < 0 || end >= getSize() || end < start)
            return null;

        ArrayList<LogEntry> result = new ArrayList<>();

        int committedSize = committedLog.size();

        // entirely in committed log
        if (end < committedSize) {
            result.addAll(committedLog.subList(start, end + 1));
            return result;
        }

        // entirely in uncommitted log
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

    public void clearUncommitted() {
        uncommittedLog.clear();
    }
}