package raft;

import java.util.HashMap;
import java.util.Map;
import cluster.Node;
import communication.Pipe;
import communication.HandOff;
import message.RaftConfig;
import com.google.gson.Gson;

/**
 * Mutable Raft runtime state for one node participating in either the
 * cluster-wide or shard-wide consensus group.
 *
 * This object stores the current term, active role, node membership sets,
 * replication indexes, pending configuration changes, and the log container
 * used to coordinate leader election, replication, and membership updates.
 */
public class RaftState {

    /** Current Raft term observed by this node. */
    public int term;

    /** Local node identifier. */
    public String id;

    /** Target node set after the next configuration transition. */
    public Map<String, Node> nextNodes;

    /** Previous voter set used during joint configuration transitions. */
    public Map<String, Node> oldNodes;

    /** Current known nodes in the group. */
    public Map<String, Node> allNodes;

    /** Learner nodes tracked as part of the Raft membership model. */
    public Map<String,Node> learners;

    /** Current voter set for this Raft group. */
    public Map<String, Node> voters;

    /** Whether the node is currently operating in a joint-config transition. */
    public boolean jointConfig;

    /** Whether a configuration change is waiting to be applied. */
    public boolean configChangePending;

    /** Log storage for this node. */
    public RaftLog log;

    /** Leader node currently known to this follower or candidate. */
    public Node leader;

    /** The candidate that this node voted for in the current term. */
    public String votedFor;

    /** Current role: follower, candidate, leader, or learner. */
    public String type;

    /** Highest log index that each follower has acknowledged. */
    public HashMap<String, Integer> matchIndex;

    /** The next log index that a leader should send to each follower. */
    public HashMap<String, Integer> nextIndex;

    /** JSON serializer used for logging and RPC serialization. */
    public Gson gson;

    /** Last stable configuration version observed by this node. */
    public int version;

    /** Version associated with the pending configuration change. */
    public int nextVersion;

    /** Whether the current group governs shard or cluster traffic. */
    public String level;

    /** Outgoing message pipe used by the log to forward committed operations. */
    public Pipe outPipe;

    /** Pipe used to acknowledge configuration changes back to the caller. */
    public Pipe callbackPipe;

    /** Whether a log entry was appended and needs a follow-up broadcast. */
    public boolean pendingLog;

    /**
     * Learners discovered during a configuration transition that still need
     * replication and promotion work.
     */
    public HashMap<String, Node> newLearners;

    /**
     * Creates a new Raft state object for the local node with the supplied
     * identifier, output pipe, Raft level, and initial membership map.
     *
     * @param nodeId local node identifier
     * @param outPipe pipe used to publish committed client operations
     * @param level Raft pipeline level for this node
     * @param configData bootstrap node membership map
     * @param ackPipe acknowledgement pipe for configuration changes
     * @param learner whether this instance should behave as a learner
     */
    public RaftState(
            String nodeId,
            Pipe outPipe,
            String level,
            Map<String, Node> configData,
            Pipe ackPipe,
            boolean learner
    ) {
        this.id = nodeId;
        this.term = 0;
        this.nextNodes = new HashMap<>(configData);
        this.oldNodes = new HashMap<>(configData);
        this.allNodes = new HashMap<>(configData);
        this.learners = new HashMap<>();
        this.voters = new HashMap<>(configData);
        this.jointConfig = false;
        this.configChangePending = false;
        this.log = new RaftLog(outPipe,this);
        this.leader = null;
        this.votedFor = null;
        this.gson = new Gson();
        this.version = 0;
        this.nextVersion = -1;
        this.level = level;
        this.outPipe = outPipe;
        this.callbackPipe = ackPipe;
        this.pendingLog = false;
        this.newLearners = new HashMap<>();
        if (learner)
            this.type = "learner";
        else
            this.type = "follower";
    }

    /**
     * Initializes the leader's follower tracking tables.
     *
     * Each node in the current membership map receives a starting match index
     * and a next send index so the leader can drive replication progress and
     * commit decisions after election victory.
     */
    public void initializeLeaderState() {
        this.matchIndex = new HashMap<>();
        this.nextIndex = new HashMap<>();

        for (String nodeId : this.allNodes.keySet()) {
            this.matchIndex.put(nodeId, -1);
            this.nextIndex.put(nodeId, this.log.getLastIdx() + 1);
        }
        checkConfigChange();
    }

    /**
     * Checks whether a pending configuration change should be scheduled as a
     * new joint-config append.
     */
    public void checkConfigChange() {
        if (configChangePending && !jointConfig && !this.log.uncommitedJointConfig &&
            nextNodes.size() < allNodes.size()) {
            appendNewConfig(nextNodes, true, true);
        }

    }

    /**
     * Returns the log file name associated with the current Raft pipeline level.
     *
     * @return cluster or shard log file path
     */
    public String getLogFilePath() {
        if ("cluster".equalsIgnoreCase(this.level)) {
            return "logs/ClusterRaft.log";
        }
        return "logs/ShardRaft.log";
    }

    /**
     * Consumes the pending-log flag exactly once and reports whether an append
     * broadcast should be retriggered.
     *
     * @return true when a new log entry should be re-broadcast, otherwise false
     */
    public boolean getPendingLog() {
        if (this.pendingLog) {
            this.pendingLog = false;
            return true;
        } else
            return false;
    }

    /**
     * Accepts a new configuration snapshot and updates the pending version and
     * learner membership state.
     *
     * When the new node set is larger, newly discovered nodes are added to the
     * tracked membership and learner lists. When the set is smaller, a config
     * change is recorded as pending so the node can later append the new
     * configuration entry.
     *
     * @param nodes incoming node map for the next configuration
     * @param nextVersion version number associated with the incoming config
     */
    public void readNewConfig(Map<String, Node> nodes, int nextVersion) {
        this.nextVersion = nextVersion;

        // If the incoming configuration is empty, do nothing
        if (nodes == null || nodes.isEmpty()) {
            return;
        }

        // If the incoming configuration is larger, add new nodes to the tracked membership and learner lists
        if (this.allNodes == null) {
            this.allNodes = new HashMap<>();
        }
        if (nodes.size() > this.allNodes.size()) {
            Map<String, Node> addedNodes = new HashMap<>(nodes);
            addedNodes.keySet().removeAll(this.allNodes.keySet());
            this.allNodes.putAll(addedNodes);
            this.learners.putAll(addedNodes);
            this.newLearners.putAll(addedNodes);

            if (this.matchIndex != null && this.nextIndex != null) {
                for (Node n : addedNodes.values()) {
                    this.matchIndex.put(n.id, -1);
                    this.nextIndex.put(n.id, 0);
                }
            }
        // If the incoming configuration is smaller, record a pending config change
        } else if (nodes.size() < this.allNodes.size()) {
            boolean configChanged = this.voters == null || !this.voters.equals(nodes);
            if (configChanged && !this.configChangePending) {
                configChangePending = true;
                nextNodes = nodes;

                if (this.type.equals("leader"))
                    appendNewConfig(nodes, true, true);
            }
        }
    }

    /**
     * Promotes any learner that has caught up to the current committed index.
     *
     * Learners that are fully replicated are folded into the next configuration
     * append, allowing the cluster to transition from learner-only tracking to
     * full voter participation when the configuration becomes stable.
     */
    public void appendLearnerPromotion() {
        if (configChangePending)
            return;

        HashMap<String, Node> newConfig = new HashMap<>();
        for (Node n : this.learners.values()) {
            if (this.matchIndex.get(n.id) >= this.log.getCommitIdx()) {
                newConfig.put(n.id, n);
            }
        }
        if (newConfig.size() > 0 && !configChangePending) {
            newConfig.putAll(this.voters);
            this.appendNewConfig(newConfig, true, true);
            this.configChangePending = true;
            return;
        }
        return;
    }

    /**
     * Appends a configuration change into the local Raft log.
     *
     * @param newConfig next membership map for the cluster or shard group
     * @param jointConfig whether the config append should preserve both old and
     *     new voter sets during the transition
     * @param pendingLog whether the appended configuration should trigger a
     *     follow-up broadcast of the current log state
     */
    public void appendNewConfig(Map<String,Node> newConfig, boolean jointConfig, boolean pendingLog) {
        HandOff.writeToFile(
            "Node " + this.id + " " + this.level + ": appending new config: " + this.gson.toJson(newConfig),
            this.getLogFilePath()
        );
        if (jointConfig) {
            // Append a joint-config entry: oldNodes should be the current voter set
            this.log.appendEntry(new RaftConfig(newConfig, this.voters, true, nextVersion), term);
        } else {
            // Append a final, non-joint config. oldNodes is null for the stable config.
            this.log.appendEntry(new RaftConfig(newConfig, null, false, nextVersion), term);
        }
        matchIndex.put(id, log.getLastIdx());
        this.pendingLog = pendingLog;
    }

    /**
     * Applies a committed configuration message to the local Raft membership
     * state.
     *
     * Joint-config messages update the old and next voter sets and put the
     * node into a transitional configuration phase. Final-config messages
     * install the stable voter set, clear the pending configuration state, and
     * remove any nodes that are no longer part of the active group.
     *
     * @param msg committed RaftConfig entry that should be materialized locally
     */
    public void proccessNewConfig(RaftConfig msg) {
        if (msg == null) {
            return;
        }

        // Apply joint-config state: set old and next node sets and mark joint mode.
        if (msg.jointConfig) {
            this.oldNodes = msg.oldNodes == null ? new HashMap<>() : new HashMap<>(msg.oldNodes);
            this.nextNodes = msg.nodes == null ? new HashMap<>() : new HashMap<>(msg.nodes);
            if (this.voters == null) {
                this.voters = new HashMap<>();
            }
            this.voters.clear();
            this.voters.putAll(this.oldNodes);
            this.voters.putAll(this.nextNodes);
            this.jointConfig = true;
            this.nextVersion = msg.version;

            // New nodes in nextNodes should be tracked as learners until final-config commits.
            for (Node n : this.nextNodes.values()) {
                this.learners.remove(n.id);
            }

            if (this.allNodes == null) {
                this.allNodes = new HashMap<>();
            }
            this.allNodes.putAll(this.oldNodes);
            this.allNodes.putAll(this.nextNodes);
        // Apply final-config state: set the stable voter set and remove any nodes that are no longer part of the group.
        } else {
            this.oldNodes = null;
            this.nextNodes = null;
            this.voters = msg.nodes == null ? new HashMap<>() : new HashMap<>(msg.nodes);
            this.jointConfig = false;
            this.version = msg.version;
            this.configChangePending = false;

            // Remove any nodes that are no longer part of the active group from the tracked membership and replication state.
            if (this.allNodes == null) {
                this.allNodes = new HashMap<>();
            }
            this.allNodes.putAll(this.voters);
            Map<String, Node> removed = new HashMap<>(this.allNodes);
            removed.keySet().removeAll(this.voters.keySet());
            for (Node n : removed.values()) {
                if (this.matchIndex != null) {
                    this.matchIndex.remove(n.id);
                }
                if (this.nextIndex != null) {
                    this.nextIndex.remove(n.id);
                }
                this.allNodes.remove(n.id);
            
                if (this.id.equals(n.id)) {
                    this.type = "learner";
                }
            }

            // Update the node's role based on whether it is still a voter or has become a learner.
            if (this.type.equals("learner") && this.voters.get(this.id) != null) {
                this.type = "follower";
            } else if (this.type.equals("follower") && this.voters.get(this.id) == null) {
                this.type = "learner";
            }
        }
    }
}
