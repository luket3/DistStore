package raft;

import java.util.ArrayList;

import communication.HandOff;
import communication.Pipe;
import message.Ack;
import message.Message;
import message.RaftConfig;
import message.Reply;

public class RaftLog {
    private ArrayList<LogEntry> committedLog;
    private ArrayList<LogEntry> uncommittedLog;
    private Pipe outPipe;
    private RaftState raftState;

    public RaftLog(Pipe outPipe, RaftState raftState) {
        this.committedLog = new ArrayList<>();
        this.uncommittedLog = new ArrayList<>();
        this.outPipe = outPipe;
        this.raftState = raftState;
    }

    public void appendEntry(Message command, int term) {
        LogEntry newEntry = new LogEntry(
                command,
                term,
                committedLog.size() + uncommittedLog.size()
        );
        uncommittedLog.add(newEntry);
    }

    public RaftConfig commitEntries(int upToIndex) {
        HandOff.writeToFile("Node " + raftState.id + " " + raftState.level + ": commiting upto index " + upToIndex, raftState.getLogFilePath());

        RaftConfig rtnValue = null;

        while (!uncommittedLog.isEmpty()
                && uncommittedLog.get(0).index <= upToIndex) {
            LogEntry entryToCommit = uncommittedLog.remove(0);
            committedLog.add(entryToCommit);
            if (entryToCommit.msg.type.equals("DictMsg") || entryToCommit.msg.type.equals("NodeMsg")) {
                Reply msg = (Reply) entryToCommit.msg;
                if (this.raftState.type != "leader") {
                    msg.client = null;
                }

                outPipe.put(msg);
            } else if (entryToCommit.msg.type.equals("RaftConfig")) {
                RaftConfig raftConfig = (RaftConfig) entryToCommit.msg;

                // Let RaftState handle joint vs final config application.
                raftState.proccessNewConfig(raftConfig);
                rtnValue = raftConfig;
                if (raftConfig.targetVersion == raftConfig.version) {
                    raftState.callbackPipe.put(new Ack(raftConfig.version));
                }
            }
        }

        return rtnValue;
    }

    public Message getLastCommittedCommand() {
        if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).msg;
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

    public void wipe() {
        committedLog.clear();
        uncommittedLog.clear();
    }
}