package raft;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import cluster.ConsistentHashMap;
import cluster.Node;
import communication.Pipe;
import communication.HandOff;
import message.MigrateData;
import message.RaftConfig;
import message.SplitRaftConfig;
import message.Update;
import message.Snapshot;
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

    /** every node that is active in current configuration */
    public Map<String, Node> activeNodes;

    /** Learner nodes tracked as part of the Raft membership model. */
    public Map<String,Node> learners;

    /** Current voter set for this Raft group. */
    public Map<String, Node> voters;

    /** during a shard spliting event this is the nodes that will be in the inital shard after the event */
    public Map<String, Node> inNodes;

    /** during a shard splitting event this is the nodes that will be in the new split shard */
    public Map<String, Node> finNodes;

    /** Whether the node is currently operating in a joint-config transition. */
    public boolean jointConfig;

    /** Wheather the pending config change is for a shard splitting operation */
    public String splitConfig;

    /** during a shard splitting event, the ring after the split (position to shard id) */
    public TreeMap<Long, String> splitRing;

    /** during a shard splitting event, the id of the shard that is splitting */
    public String inShardId;

    /** during a shard splitting event, the id of the shard being created */
    public String newShardId;

    /** ids of shard data hand-overs sent by this node that are still waiting for an acknowledgement */
    public final Set<String> pendingMigrations = ConcurrentHashMap.newKeySet();

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
     * @param stateMachine state machine committed client operations are applied to
     * @param level Raft pipeline level for this node
     * @param configData bootstrap node membership map
     * @param ackPipe acknowledgement pipe for configuration changes
     * @param learner whether this instance should behave as a learner
     */
    public RaftState(
            String nodeId,
            ReplicatedStateMachine stateMachine,
            String level,
            Map<String, Node> configData,
            Pipe ackPipe,
            boolean learner
    ) {
        this.id = nodeId;
        this.term = 0;
        this.nextNodes = new HashMap<>(configData);
        this.oldNodes = new HashMap<>(configData);
        this.activeNodes = new HashMap<>(configData);
        this.learners = new HashMap<>();
        this.voters = new HashMap<>(configData);
        this.inNodes = new HashMap<>();
        this.finNodes = new HashMap<>();
        this.jointConfig = false;
        this.splitConfig = "false";
        this.configChangePending = false;
        this.log = new RaftLog(stateMachine, this);
        this.leader = null;
        this.votedFor = null;
        this.gson = new Gson();
        this.version = 0;
        this.nextVersion = -1;
        this.level = level;
        this.callbackPipe = ackPipe;
        this.pendingLog = false;
        this.newLearners = new HashMap<>();
        if (learner)
            this.type = "learner";
        else
            this.type = "follower";

        // Restore from a previously saved snapshot if one exists on disk
        Snapshot restored = this.log.restoreFromDisk();
        if (restored != null) {
            this.term = restored.term;
            this.votedFor = restored.votedFor;
            if (restored.activeNodes != null && !restored.activeNodes.isEmpty())
                this.activeNodes = new HashMap<>(restored.activeNodes);
            applySnapshotConfig(restored);
        }
    }

    /**
     * Adopts the Raft membership configuration stored in a snapshot.
     *
     * Configuration entries covered by a snapshot are no longer in the log, so
     * the snapshot is the only record of them. The configuration is applied
     * only if it is at least as new as the one the node already holds.
     *
     * @param snapshot snapshot restored from disk or installed from the leader
     */
    public void applySnapshotConfig(Snapshot snapshot) {
        if (snapshot == null || snapshot.version < this.version)
            return;

        this.version = snapshot.version;
        if (snapshot.voters == null || snapshot.voters.isEmpty())
            return;

        this.voters = new HashMap<>(snapshot.voters);
        this.oldNodes = new HashMap<>(snapshot.voters);
        this.nextNodes = new HashMap<>(snapshot.voters);
        if (snapshot.activeNodes != null)
            this.activeNodes.putAll(snapshot.activeNodes);
        this.activeNodes.putAll(this.voters);
        for (String nodeId : this.voters.keySet())
            this.learners.remove(nodeId);

        // A node is a voter only if the snapshot's configuration says so
        if (this.voters.containsKey(this.id)) {
            if (this.type.equals("learner"))
                this.type = "follower";
        } else if (!this.type.equals("learner")) {
            this.type = "learner";
        }
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

        for (String nodeId : this.activeNodes.keySet()) {
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
            nextNodes.size() < activeNodes.size()) {
            appendNewConfig(true, true);
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
     * Updates values assisated with a Config change request
     *
     * @param nodes incoming node map for the next configuration
     * @param nextVersion version number associated with the incoming config
     */
    public boolean readNewConfigHelp(Map<String, Node> nodes, int nextVersion) {
        this.nextVersion = nextVersion;

        // If the incoming configuration is empty, do nothing
        if (nodes == null || nodes.isEmpty()) {
            return false;
        }

        // If the incoming configuration is larger, add new nodes to the tracked membership and learner lists
        if (nodes.size() > this.activeNodes.size()) {
            Map<String, Node> addedNodes = new HashMap<>(nodes);
            addedNodes.keySet().removeAll(this.activeNodes.keySet());
            this.activeNodes.putAll(addedNodes);
            this.learners.putAll(addedNodes);
            this.newLearners.putAll(addedNodes);

            if (this.matchIndex != null && this.nextIndex != null) {
                for (Node n : addedNodes.values()) {
                    this.matchIndex.put(n.id, -1);
                    this.nextIndex.put(n.id, 0);
                }
            }
        // If the incoming configuration is smaller, record a pending config change
        } else if (nodes.size() < this.activeNodes.size()) {
            boolean configChanged = this.voters == null || !this.voters.equals(nodes);
            if (configChanged && !this.configChangePending) {
                configChangePending = true;
                nextNodes = nodes;

                if (this.type.equals("leader"))
                    return true;
            }
        }
        return false;
    }

    /**
     * updates raft state for a pending cluster configuration change split event
     * 
     * @param nodes incoming node map for the next configuration
     * @param nextVersion version number associated with the incoming config
     */
    public void readNewConfig(Map<String, Node> nodes, int nextVersion) {
        this.splitConfig = "false";
        if (readNewConfigHelp(nodes, nextVersion)) {
            appendNewConfig(nodes, true, true);
        }
    }

    /**
     * updates raft state for a pending Shard split event
     * 
     * @param nodes incoming node map for the next configuration
     * @param inNodes all nodes in the inital shard or shard that has split
     * @param finNodes all nodes in the new shard that was split from inital shard
     * @param nextVersion version number associated with the incoming config
     */
    public void readNewConfig(Map<String, Node> nodes, Map<String,Node> inNodes, Map<String,Node> finNodes, int nextVersion) {
        this.splitConfig = "pending";
        this.inNodes = inNodes;
        this.finNodes = finNodes;
        if (readNewConfigHelp(nodes, nextVersion)) {
            appendNewConfig(nodes, inNodes, finNodes, true, true);
        }
    }

    /**
     * Resets the Raft state for a shard removal event.
     *
     * The node first hands its keys to the shards that now own them, then
     * clears its log and key/value data and becomes a learner of its new
     * shard. The new shard's leader then brings it up to date, by snapshot
     * or by replaying the log, so it ends up with that shard's state.
     *
     * @param update shard removal update; its nodes are the new shard's members
     */
    public void shardRemoval(Update update) {
        Map<String, Node> nodes = update.nodes;
        handOverKeys(update);
        this.log.getStateMachine().retainKeys(key -> false);
        this.leader = null;
        this.version = 0;
        this.term = 0;
        this.activeNodes = new HashMap<>(nodes);
        this.voters = new HashMap<>(nodes);
        this.jointConfig = false;
        this.splitConfig = "false";
        this.configChangePending = true;
        this.log.wipe();
        this.type = "learner";
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
        if (newConfig.size() == this.learners.size() && newConfig.size() > 0 && !configChangePending ) {
            newConfig.putAll(this.voters);
            nextNodes = newConfig;
            this.appendNewConfig(true, true);
            this.configChangePending = true;
            return;
        }
        return;
    }

    /**
     * if a node is in leader role step down to follower
     */
    public void stepDown() {
        if (this.type != "learner") {
            this.type = "follower";
        }
    }

    /**
     * append a config change entry to the replicated log
     * 
     * @param entry to commit to log
     * @param pendingLog whether the appended configuration should trigger a
     *     follow-up broadcast of the current log state
     */
    public void appendNewConfigHelp(RaftConfig entry, boolean pendingLog) {
        HandOff.writeToFile(
            "Node " + this.id + " " + this.level + ": appending new config: " + this.gson.toJson(entry),
            this.getLogFilePath()
        );
        this.log.appendEntry(entry, term);
        matchIndex.put(id, log.getLastIdx());
        this.pendingLog = pendingLog;
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
        if (jointConfig) {
            // Append a joint-config entry: oldNodes should be the current voter set
            appendNewConfigHelp(new RaftConfig(newConfig, this.voters, true, nextVersion), pendingLog);
        } else {
            // Append a final, non-joint config. oldNodes is null for the stable config.
            appendNewConfigHelp(new RaftConfig(newConfig, null, false, nextVersion), pendingLog);
        }
    }

    /**
     * Appends a Shard split configuration change into the local raft log
     * 
     * @param allNodes every node in both the intial shard and the new shard after splitting
     * @param inNodes all nodes in the inital shard or shard that has split
     * @param finNodes all nodes in the new shard that was split from inital shard
     * @param jointConfig whether the config append should preserve both old and
     *     new voter sets during the transition
     * @param pendingLog whether the appended configuration should trigger a
     *     follow-up broadcast of the current log state
     */
    public void appendNewConfig(Map<String,Node> allNodes, Map<String,Node> inNodes, Map<String,Node> finNodes, 
                                boolean jointConfig, boolean pendingLog) {
        if (jointConfig) {
            // Append a joint-config entry: oldNodes should be the current voter set
            appendNewConfigHelp(new SplitRaftConfig(allNodes, inNodes, finNodes, this.voters, true, nextVersion,
                splitRing, inShardId, newShardId), pendingLog);
        } else {
            // Append a final, non-joint config. oldNodes is null for the stable config.
            appendNewConfigHelp(new SplitRaftConfig(allNodes, inNodes, finNodes, null, false, nextVersion,
                splitRing, inShardId, newShardId), pendingLog);
        }
    }

    /**
     * Appends a cluster configuration entry to the local log
     * 
     * @param jointConfig whether the config append should preserve both old and
     *     new voter sets during the transition
     * @param pendingLog whether the appended configuration should trigger a
     *     follow-up broadcast of the current log state
     */
    public void appendNewConfig(boolean jointConfig, boolean pendingLog) {
        if (this.splitConfig.equals("pending")) {
            appendNewConfig(nextNodes, inNodes, finNodes, jointConfig, pendingLog);
        } else {
            appendNewConfig(nextNodes,jointConfig, pendingLog);
        }
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
        if (msg == null || msg.version <= this.version) {
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

            // Remember the split details from the log, so a node that becomes
            // leader mid-split appends the same final config
            if (msg.type.equals("SplitRaftConfig")) {
                SplitRaftConfig castMsg = (SplitRaftConfig) msg;
                this.inNodes = castMsg.inNodes;
                this.finNodes = castMsg.newNodes;
                this.splitRing = castMsg.ring;
                this.inShardId = castMsg.inShardId;
                this.newShardId = castMsg.newShardId;
            }

            // New nodes in nextNodes should be tracked as learners until final-config commits.
            for (Node n : this.nextNodes.values()) {
                this.learners.remove(n.id);
            }

            this.activeNodes.putAll(this.oldNodes);
            this.activeNodes.putAll(this.nextNodes);
        // Apply final-config state: set the stable voter set and remove any nodes that are no longer part of the group.
        } else {
            // determine the nodes present in the final config
            Map<String, Node> newNodes;
            if (msg.type.equals("SplitRaftConfig")) {
                SplitRaftConfig castMsg = (SplitRaftConfig) msg;
                String myShard;
                if (castMsg.inNodes.containsKey(this.id)) {
                    newNodes = castMsg.inNodes;
                    myShard = castMsg.inShardId;
                } else {
                    newNodes = castMsg.newNodes;
                    myShard = castMsg.newShardId;
                }
                keepOwnedKeys(castMsg.ring, myShard);
            } else {
                newNodes = msg.nodes;
            }

            this.voters = newNodes == null ? new HashMap<>() : new HashMap<>(newNodes);
            this.jointConfig = false;
            this.version = msg.version;
            this.configChangePending = false;

            // Remove any nodes that are no longer part of the active group from the tracked membership and replication state.
            this.activeNodes.putAll(this.voters);
            this.oldNodes = new HashMap<>(this.activeNodes);
            Map<String, Node> removed = new HashMap<>(this.activeNodes);
            removed.keySet().removeAll(this.voters.keySet());
            for (Node n : removed.values()) {
                this.activeNodes.remove(n.id);
            
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

            if (this.splitConfig.equals("pending") && this.type.equals("leader")) {
                this.splitConfig = "finalized";
            } else if (this.splitConfig.equals("pending")) {
                this.splitConfig = "false";
            }
        }
    }

    /**
     * After a split, drops every key that the local node's shard no longer
     * owns. Both halves held all of the shard's keys before the split, so each
     * half just keeps its own. This runs while the final split config is
     * committed, at the same log index on every replica, so all replicas
     * (and any snapshot taken afterwards) end up with the same keys.
     *
     * @param ring ring after the split, as ring position to shard id
     * @param myShard id of the shard the local node belongs to after the split
     */
    private void keepOwnedKeys(TreeMap<Long, String> ring, String myShard) {
        if (ring == null || myShard == null) {
            HandOff.writeToFile(
                "Node " + this.id + " " + this.level + ": split config has no ring, keeping all keys",
                this.getLogFilePath()
            );
            return;
        }

        HandOff.writeToFile(
            "Node " + this.id + " " + this.level + ": split finalised, keeping keys owned by " + myShard,
            this.getLogFilePath()
        );
        this.log.getStateMachine().retainKeys(key -> {
            try {
                return myShard.equals(ConsistentHashMap.ownerOf(ring, key));
            } catch (Exception e) {
                return true; // cannot hash: keep rather than lose data
            }
        });
    }

    /**
     * Hands the local node's keys to the shards that now own them after its
     * shard was removed.
     *
     * Keys are grouped by owning shard on the new ring and each group is sent
     * to that shard in the background, retrying until the shard's leader
     * acknowledges it has committed the data.
     *
     * @param update shard removal update carrying the new ring and shard members
     */
    private void handOverKeys(Update update) {
        Map<String, String> data = this.log.getStateMachine().exportKeys();
        if (data.isEmpty())
            return;
        if (update.ring == null || update.shardMembers == null) {
            HandOff.writeToFile(
                "Node " + this.id + " " + this.level + ": shard removed but no ring supplied, " + data.size() + " keys not handed over",
                this.getLogFilePath()
            );
            return;
        }

        // Group the keys by the shard that owns them on the new ring
        Map<String, Map<String, String>> byShard = new HashMap<>();
        for (Map.Entry<String, String> e : data.entrySet()) {
            try {
                String owner = ConsistentHashMap.ownerOf(update.ring, e.getKey());
                byShard.computeIfAbsent(owner, k -> new HashMap<>()).put(e.getKey(), e.getValue());
            } catch (Exception ex) {
                HandOff.writeToFile("Node " + this.id + ": cannot hash key " + e.getKey() + ": " + ex, this.getLogFilePath());
            }
        }

        // The acknowledgement comes back to this node's address
        Node self = update.nodes != null ? update.nodes.get(this.id) : null;
        if (self == null)
            self = this.activeNodes.get(this.id);

        for (Map.Entry<String, Map<String, String>> group : byShard.entrySet()) {
            String shardId = group.getKey();
            Map<String, Node> shardNodes = update.shardMembers.getOrDefault(shardId, new HashMap<>());
            List<Node> targets = ShardMigrator.orderTargets(shardNodes, this.activeNodes, this.id);

            MigrateData migration = new MigrateData(UUID.randomUUID().toString(), shardId, group.getValue(), self);
            HandOff.writeToFile(
                "Node " + this.id + " " + this.level + ": handing " + group.getValue().size() + " keys to " + shardId,
                this.getLogFilePath()
            );
            ShardMigrator.start(migration, targets, this.pendingMigrations, this.getLogFilePath());
        }
    }
}
