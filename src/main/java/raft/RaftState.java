package raft;
import java.util.HashMap;
import java.util.Map;

import cluster.Node;
import communication.Pipe;
import message.RaftConfig;

import com.google.gson.Gson;

public class RaftState {

    public int term;
    public String id;

    public Map<String, Node> nextNodes;
    public Map<String, Node> oldNodes;
    public Map<String, Node> allNodes;
    public Map<String,Node> aliveLearners;
    public Map<String,Node> deadLearners;
    public Map<String, Node> voters;
    public boolean jointConfig;
    public boolean configChangePending;

    public RaftLog log;
    public Node leader;
    public String votedFor;
    public String type;
    public HashMap<String, Integer> matchIndex;
    public HashMap<String, Integer> nextIndex;
    public Gson gson;
    public int version;
    public int nextVersion;
    public String level;
    public Pipe outPipe;
    public Pipe callbackPipe;

    public RaftState(
            String nodeId,
            Pipe outPipe,
            String level,
            Map<String, Node> configData,
            Pipe ackPipe
    ) {
        this.nextNodes = null;
        this.oldNodes = null;
        this.aliveLearners = new HashMap<>();
        this.deadLearners = new HashMap<>();
        this.allNodes = null;
        this.voters = null;
        this.jointConfig = false;
        this.id = nodeId;
        this.log = new RaftLog(outPipe,this);
        this.term = 0;
        this.votedFor = null;
        this.leader = null;
        this.type = "follower";
        this.gson = new Gson();
        this.version = -1;
        this.nextVersion = -1;
        this.level = level;
        this.outPipe = outPipe;
        this.callbackPipe = ackPipe;
        this.configChangePending = false;
        initConfig(configData, 0);
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
    }

    public String getLogFilePath() {
        if ("cluster".equalsIgnoreCase(this.level)) {
            return "logs/ClusterRaft.log";
        }
        return "logs/ShardRaft.log";
    }

    public void initConfig(Map<String, Node> nodes, int version) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        if (nodes.get(this.id) == null) {
            this.type = "learner";
        }
        changeConfig(nodes, version);
    }

    public void changeConfig(Map<String, Node> nodes, int version) {
        if (nodes == null) {
            this.oldNodes = new HashMap<>();
            this.allNodes = new HashMap<>();
            this.voters = new HashMap<>();
            this.jointConfig = false;
            return;
        }

        this.oldNodes = new HashMap<>(nodes);
        this.allNodes = new HashMap<>(nodes);
        this.voters = new HashMap<>(nodes);
        this.jointConfig = false;
        this.version = version;
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
            this.aliveLearners.putAll(addedNodes);

            if (this.matchIndex != null && this.nextIndex != null) {
                for (Node n : addedNodes.values()) {
                    this.matchIndex.put(n.id, -1);
                    this.nextIndex.put(n.id, 0);
                }
            }
        } else if (nodes.size() < this.allNodes.size()) {
            boolean configChanged = this.voters == null || !this.voters.equals(nodes);
            if (this.type.equals("leader") && configChanged && !this.configChangePending) {
                appendNewConfig(nodes, true);
                configChangePending = true;
            }
        }
    }

    public boolean appendLearnerPromotion() {
        if (configChangePending)
            return false;

        HashMap<String, Node> newConfig = new HashMap<>();
        for (Node n : this.aliveLearners.values()) {
            if (this.matchIndex.get(n.id) >= this.log.getCommitIdx()) {
                newConfig.put(n.id, n);
            }
        }
        if (newConfig.size() > 0) {
            newConfig.putAll(this.voters);
            this.appendNewConfig(newConfig, true);
            this.configChangePending = true;
            return true;
        }

        return false;
    }

    public void appendNewConfig(Map<String,Node> newConfig, boolean jointConfig) {
        if (jointConfig) {
            // Append a joint-config entry: oldNodes should be the current voter set
            this.log.appendEntry(new RaftConfig(newConfig, this.voters, true, nextVersion), term);
        } else {
            // Append a final, non-joint config. oldNodes is null for the stable config.
            this.log.appendEntry(new RaftConfig(newConfig, null, false, nextVersion), term);
        }
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
                this.aliveLearners.remove(n.id);
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
                this.deadLearners.put(n.id, n);
            }

            if (this.type.equals("learner") && this.voters.get(this.id) != null) {
                this.type = "follower";
            }
        }
    }
}
