package raft;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import cluster.Node;
import communication.HandOff;
import communication.Pipe;
import message.Ack;
import message.DictMsg;
import message.Message;
import message.NodeMsg;
import message.RaftConfig;
import message.Reply;
import message.Snapshot;

/**
 * Local Raft log container that separates committed and uncommitted entries.
 *
 * The log stores replicated operations in a sequence of LogEntry records, keeps
 * the committed prefix immutable for the local node, and exposes helper methods
 * for append, commit, lookup, range read, and truncation.
 *
 * Once {@link #SNAPSHOT_THRESHOLD} committed entries have accumulated, the
 * committed prefix is compacted into a {@link Snapshot} that is persisted to
 * disk. Log indexes stay absolute: the first entry held in memory has index
 * {@code snapshotLastIndex + 1}.
 */
public class RaftLog {
    /**
     * Number of committed entries held in memory before they are compacted
     * into a snapshot. Override with {@code -Draft.snapshotThreshold=N}.
     */
    public static final int SNAPSHOT_THRESHOLD = Integer.getInteger("raft.snapshotThreshold", 100);

    /** Directory snapshot files are written to. */
    public static final String SNAPSHOT_DIR = "snapshots";

    /** Committed log entries that have not yet been compacted into a snapshot. */
    private ArrayList<LogEntry> committedLog;

    /** Log entries accepted by the leader but not yet committed. */
    private ArrayList<LogEntry> uncommittedLog;

    /** Output pipe used to publish committed client or membership messages. */
    private Pipe outPipe;

    /** Shared Raft state associated with this log instance. */
    private RaftState raftState;

    /** Tracks whether a joint configuration update is still waiting to complete. */
    public boolean uncommitedJointConfig;

    /** Index of the last entry covered by the current snapshot, or -1 if none. */
    private int snapshotLastIndex;

    /** Term of the last entry covered by the current snapshot, or 0 if none. */
    private int snapshotLastTerm;

    /** Key-value state produced by every committed DictMsg, used to build snapshots. */
    private HashMap<String, String> kvState;

    /** Committed cluster membership changes in order, used to build snapshots. */
    private ArrayList<Snapshot.MembershipOp> membershipOps;

    /** Most recent snapshot taken or installed, sent to lagging followers. */
    private Snapshot latestSnapshot;

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
        this.snapshotLastIndex = -1;
        this.snapshotLastTerm = 0;
        this.kvState = new HashMap<>();
        this.membershipOps = new ArrayList<>();
        this.latestSnapshot = null;
    }

    /**
     * Returns the absolute index of the first entry held in memory.
     *
     * @return index of the first in-memory entry
     */
    private int base() {
        return snapshotLastIndex + 1;
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
                base() + committedLog.size() + uncommittedLog.size()
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
            applyToSnapshotState(entryToCommit.msg);

            // Publish committed client or membership messages to the output pipe
            if (entryToCommit.msg.type.equals("DictMsg") || entryToCommit.msg.type.equals("NodeMsg")) {
                Reply msg = (Reply) entryToCommit.msg;
                if (this.raftState.type != "leader") {
                    msg.client = null;
                }
                outPipe.put(msg);
            // If the committed entry is a RaftConfig, let the caller know so it can
            // update its local configuration state.
            } else if (entryToCommit.msg.type.equals("RaftConfig") || entryToCommit.msg.type.equals("SplitRaftConfig")) {
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

        maybeTakeSnapshot();
        return rtnValue;
    }

    /**
     * Folds a committed message into the state that snapshots are built from.
     *
     * @param msg committed log payload
     */
    private void applyToSnapshotState(Message msg) {
        if (msg.type.equals("DictMsg")) {
            DictMsg dictMsg = (DictMsg) msg;
            if ("Put".equals(dictMsg.action))
                kvState.put(dictMsg.key, dictMsg.value);
            else if ("Delete".equals(dictMsg.action))
                kvState.remove(dictMsg.key);
        } else if (msg.type.equals("NodeMsg")) {
            // Mirrors the changes ClusterState accepts so replaying them gives the same ring
            NodeMsg nodeMsg = (NodeMsg) msg;
            if (nodeMsg.node == null)
                return;
            if ("Add".equals(nodeMsg.action) && nodeMsg.node.ip != null && nodeMsg.node.port != -1) {
                membershipOps.add(new Snapshot.MembershipOp(
                    "Add", new Node(nodeMsg.node.id, nodeMsg.node.ip, nodeMsg.node.port)));
            } else if ("Remove".equals(nodeMsg.action)) {
                membershipOps.add(new Snapshot.MembershipOp(
                    "Remove", new Node(nodeMsg.node.id, null, -1)));
            }
        }
    }

    /**
     * Compacts the committed log into a snapshot once it reaches the configured
     * threshold. Compaction is deferred while a joint configuration is active
     * so a snapshot never captures a half-applied membership change.
     */
    private void maybeTakeSnapshot() {
        if (committedLog.size() < SNAPSHOT_THRESHOLD || raftState.jointConfig)
            return;

        LogEntry last = committedLog.get(committedLog.size() - 1);
        snapshotLastIndex = last.index;
        snapshotLastTerm = last.term;
        committedLog.clear();

        latestSnapshot = new Snapshot(
            snapshotLastIndex,
            snapshotLastTerm,
            raftState.term,
            raftState.votedFor,
            raftState.voters,
            raftState.activeNodes,
            kvState,
            membershipOps,
            raftState.version
        );
        persistSnapshot(latestSnapshot);

        HandOff.writeToFile(
            "Node " + raftState.id + " " + raftState.level + ": took snapshot up to index " + snapshotLastIndex
                + " (term " + snapshotLastTerm + ")",
            raftState.getLogFilePath()
        );
    }

    /**
     * Installs a snapshot received from the leader.
     *
     * Entries covered by the snapshot are discarded. If the local log already
     * holds the snapshot's last entry with a matching term, the entries after
     * it are retained; otherwise the whole log is replaced. The installed
     * snapshot is persisted and pushed to the output pipe so the state machine
     * can replace its state.
     *
     * @param snapshot snapshot sent by the leader
     * @return true when the snapshot was installed, false when the local log
     *     already commits past it
     */
    public boolean installSnapshot(Snapshot snapshot) {
        if (snapshot == null || snapshot.lastIncludedIndex <= getCommitIdx())
            return false;

        // Keep any entries following the snapshot if the log agrees at its last index
        ArrayList<LogEntry> suffix = new ArrayList<>();
        if (getTerm(snapshot.lastIncludedIndex) == snapshot.lastIncludedTerm) {
            for (int i = snapshot.lastIncludedIndex + 1; i <= getLastIdx(); i++)
                suffix.add(get(i));
        }

        committedLog.clear();
        uncommittedLog.clear();
        uncommittedLog.addAll(suffix);
        uncommitedJointConfig = false;
        for (LogEntry e : suffix) {
            if (e.msg.type.equals("RaftConfig") && ((RaftConfig) e.msg).jointConfig)
                uncommitedJointConfig = true;
        }

        loadSnapshotState(snapshot);
        persistSnapshot(snapshot);
        outPipe.put(snapshot);

        HandOff.writeToFile(
            "Node " + raftState.id + " " + raftState.level + ": installed snapshot from leader up to index "
                + snapshotLastIndex,
            raftState.getLogFilePath()
        );
        return true;
    }

    /**
     * Restores the log from the snapshot file on disk, if one exists. The
     * restored snapshot is pushed to the output pipe so the state machine
     * starts from the saved state.
     *
     * @return the restored snapshot, or null when no snapshot exists
     */
    public Snapshot restoreFromDisk() {
        Path path = getSnapshotPath();
        if (!Files.exists(path))
            return null;

        try {
            String json = Files.readString(path, StandardCharsets.UTF_8);
            Snapshot snapshot = raftState.gson.fromJson(json, Snapshot.class);
            if (snapshot == null)
                return null;

            committedLog.clear();
            uncommittedLog.clear();
            loadSnapshotState(snapshot);
            outPipe.put(snapshot);

            HandOff.writeToFile(
                "Node " + raftState.id + " " + raftState.level + ": restored snapshot from " + path
                    + " up to index " + snapshotLastIndex,
                raftState.getLogFilePath()
            );
            return snapshot;
        } catch (Exception e) {
            HandOff.writeToFile(
                "Node " + raftState.id + " " + raftState.level + ": failed to restore snapshot " + path + ": " + e,
                raftState.getLogFilePath()
            );
            return null;
        }
    }

    /**
     * Replaces the log's snapshot bookkeeping with the supplied snapshot.
     *
     * @param snapshot snapshot to adopt
     */
    private void loadSnapshotState(Snapshot snapshot) {
        snapshotLastIndex = snapshot.lastIncludedIndex;
        snapshotLastTerm = snapshot.lastIncludedTerm;
        kvState = snapshot.kvStore == null ? new HashMap<>() : new HashMap<>(snapshot.kvStore);
        membershipOps = snapshot.membershipOps == null ? new ArrayList<>() : new ArrayList<>(snapshot.membershipOps);
        latestSnapshot = snapshot;
    }

    /**
     * Writes the snapshot to disk, replacing any previous snapshot atomically.
     *
     * @param snapshot snapshot to persist
     */
    private void persistSnapshot(Snapshot snapshot) {
        Path path = getSnapshotPath();
        try {
            Files.createDirectories(path.getParent());
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, raftState.gson.toJson(snapshot), StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            HandOff.writeToFile(
                "Node " + raftState.id + " " + raftState.level + ": failed to persist snapshot " + path + ": " + e,
                raftState.getLogFilePath()
            );
        }
    }

    /**
     * Returns the snapshot file used by this node and Raft level.
     *
     * @return path of the snapshot file
     */
    public Path getSnapshotPath() {
        return Paths.get(SNAPSHOT_DIR, raftState.id + "-" + raftState.level + ".json");
    }

    /**
     * Returns the most recent snapshot taken or installed.
     *
     * @return latest snapshot, or null when none exists
     */
    public Snapshot getLatestSnapshot() {
        return latestSnapshot;
    }

    /**
     * Returns the index of the last entry covered by the current snapshot.
     *
     * @return last snapshotted index, or -1 when no snapshot exists
     */
    public int getSnapshotLastIndex() {
        return snapshotLastIndex;
    }

    /**
     * Returns the message payload stored in the newest committed log entry.
     *
     * @return latest committed message, or null when no committed entries are
     *     held in memory
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
     * @return latest committed index, or -1 when nothing has been committed
     */
    public int getCommitIdx() {
        if (!committedLog.isEmpty()) {
            return committedLog.get(committedLog.size() - 1).index;
        }
        return snapshotLastIndex; // Everything committed is in the snapshot
    }

    /**
     * Returns the highest index currently present in the combined log.
     *
     * @return last available log index, or -1 when the log is empty
     */
    public int getLastIdx() {
        return base() + committedLog.size() + uncommittedLog.size() - 1;
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
        return snapshotLastTerm;
    }

    /**
     * Returns the term of the entry at the supplied index, including the last
     * entry covered by the snapshot.
     *
     * @param index absolute log index
     * @return the entry's term, 0 for index -1 on an empty log, or -1 when the
     *     index is unknown or has been compacted away
     */
    public int getTerm(int index) {
        if (index == snapshotLastIndex)
            return snapshotLastTerm;
        LogEntry entry = get(index);
        return entry == null ? -1 : entry.term;
    }

    /**
     * Returns the logical size of the log, including compacted entries.
     *
     * @return current log size
     */
    public int getSize() {
        return getLastIdx() + 1;
    }

    /**
     * Returns the log entry at the supplied logical index.
     *
     * @param index zero-based log position to read
     * @return matching LogEntry, or null when the index is outside the current
     *     log range or has been compacted into the snapshot
     */
    public LogEntry get(int index) {
        int pos = index - base();
        if (pos < 0)
            return null;
        if (pos < committedLog.size()) {
            return committedLog.get(pos);
        } else {
            int uncommittedIndex = pos - committedLog.size();
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
        if (max <= getCommitIdx()) {
            clearUncommitted();
            return;
        }
        if (max >= getLastIdx())
            return;
        int keep = max - (base() + committedLog.size()) + 1;
        uncommittedLog.subList(keep, uncommittedLog.size()).clear();
    }

    /**
     * Returns a contiguous slice of entries from the supplied inclusive range.
     *
     * The range may span both committed and uncommitted regions. If the range
     * is invalid or reaches into the snapshot, the method returns null.
     *
     * @param start first index in the requested range
     * @param end last index in the requested range
     * @return list of entries covering the requested range, or null when the
     *     range is outside the valid log bounds
     */
    public ArrayList<LogEntry> get(int start, int end) {
        // Validate
        if (start < base() || end > getLastIdx() || end < start)
            return null;

        int s = start - base();
        int e = end - base();

        // Case 1: Entirely in committed log
        ArrayList<LogEntry> result = new ArrayList<>();
        int committedSize = committedLog.size();
        if (e < committedSize) {
            result.addAll(committedLog.subList(s, e + 1));
            return result;
        }

        // Case 2: Entirely in uncommitted log
        if (s >= committedSize) {
            result.addAll(uncommittedLog.subList(s - committedSize, e - committedSize + 1));
            return result;
        }

        // Case 3: Spans committed → uncommitted
        result.addAll(committedLog.subList(s, committedSize));
        result.addAll(uncommittedLog.subList(0, e - committedSize + 1));
        return result;
    }

    /**
     * Removes all uncommitted entries from the current log view.
     */
    public void clearUncommitted() {
        uncommittedLog.clear();
    }

    /**
     * Clears the whole log, including the snapshot and its file on disk, so a
     * restart does not restore state the node no longer owns.
     */
    public void wipe() {
        committedLog.clear();
        uncommittedLog.clear();
        snapshotLastIndex = -1;
        snapshotLastTerm = 0;
        kvState = new HashMap<>();
        membershipOps = new ArrayList<>();
        latestSnapshot = null;
        try {
            Files.deleteIfExists(getSnapshotPath());
        } catch (Exception e) {
            HandOff.writeToFile(
                "Node " + raftState.id + " " + raftState.level + ": failed to delete snapshot: " + e,
                raftState.getLogFilePath()
            );
        }
    }
}
