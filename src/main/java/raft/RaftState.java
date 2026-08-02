package raft;
import java.util.HashMap;
import java.util.Map;

import cluster.Node;
import communication.Pipe;
import message.RaftConfig;

import com.google.gson.Gson;

/**
 * Mutable Raft runtime state for one node participating in either the
 * cluster-wide or shard-wide consensus group.
 *
 * <p>This object stores the node's current term, voting configuration,
 * commit indexes, and the per-node metadata needed to drive leader election
 * and configuration changes.</p>
 */
public class RaftState {

    /**
     * Current Raft term observed by this node.
     */
    public int term;

    /**
     * Local node identifier.
     */
    public String id;

    /**
     * Target node set after the next configuration transition.
     */
    public Map<String, Node> nextNodes;

    /**
     * Previous voter set used during joint configuration transitions.
     */
    public Map<String, Node> oldNodes;

    /**
     * Current known nodes in the group.
     */
    public Map<String, Node> allNodes;

    /**
     * Learner nodes tracked as part of the Raft membership model.
     */
    public Map<String,Node> learners;

    /**
     * Current voter set for this Raft group.
     */
    public Map<String, Node> voters;

    /**
     * Whether the node is currently operating in a joint-config transition.
     */
    public boolean jointConfig;

    /**
     * Whether a configuration change is waiting to be applied.
     */
    public boolean configChangePending;

    /**
     * Log storage for this node.
     */
    public RaftLog log;

    /**
     * Leader node currently known to this follower or candidate.
     */
    public Node leader;

    /**
     * The candidate that this node voted for in the current term.
     */
    public String votedFor;

    /**
     * Current role: follower, candidate, leader, or learner.
     */
    public String type;

    /**
     * Highest log index that each follower has acknowledged.
     */
    public HashMap<String, Integer> matchIndex;

    /**
     * The next log index that a leader should send to each follower.
     */
    public HashMap<String, Integer> nextIndex;

    /**
     * JSON serializer used for logging and RPC serialization.
     */
    public Gson gson;

    /**
     * Last stable configuration version observed by this node.
     */
    public int version;

    /**
     * Version associated with the pending configuration change.
     */
    public int nextVersion;

    /**
     * Whether the current group governs shard or cluster traffic.
     */
    public String level;

    /**
     * Outgoing message pipe used by the log to forward committed operations.
     */
    public Pipe outPipe;

    /**
     * Pipe used to acknowledge configuration changes back to the caller.
     */
    public Pipe callbackPipe;

    /**
     * Whether a log entry was appended and needs a follow-up broadcast.
     */
    public boolean pendingLog;

    /**
     * Learners discovered during a configuration transition that still need
     * replication and promotion work.
     */
    public HashMap<String, Node> newLearners;

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
     * Initializes the matchIndex and nextIndex for leader state.
     * Should be called when a node becomes leader.
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

    public void checkConfigChange() {
        if (configChangePending && !jointConfig && !this.log.uncommitedJointConfig &&
            nextNodes.size() < allNodes.size()) {
            appendNewConfig(nextNodes, true, true);
        }

    }

    public String getLogFilePath() {
        if ("cluster".equalsIgnoreCase(this.level)) {
            return "logs/ClusterRaft.log";
        }
        return "logs/ShardRaft.log";
    }

    public boolean getPendingLog() {
        if (this.pendingLog) {
            this.pendingLog = false;
            return true;
        } else
            return false;
    }

    public void readNewConfig(Map<String, Node> nodes, int nextVersion) {
        this.nextVersion = nextVersion;

        if (nodes == null || nodes.isEmpty()) {
            return;
        }

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

    public void appendNewConfig(Map<String,Node> newConfig, boolean jointConfig, boolean pendingLog) {
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

    public void proccessNewConfig(RaftConfig msg) {
        if (msg == null) {
            return;
        }

        if (msg.jointConfig) {
            // Apply joint-config state: set old and next node sets and mark joint mode.
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
        } else {
            this.oldNodes = null;
            this.nextNodes = null;
            this.voters = msg.nodes == null ? new HashMap<>() : new HashMap<>(msg.nodes);
            this.jointConfig = false;
            this.version = msg.version;
            this.configChangePending = false;

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

            if (this.type.equals("learner") && this.voters.get(this.id) != null) {
                this.type = "follower";
            }
        }
    }
}
