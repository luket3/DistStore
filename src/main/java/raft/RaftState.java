package raft;
import java.util.HashMap;
import java.util.Map;

import cluster.Node;
import communication.Pipe;
import message.UpdateShard;

import com.google.gson.Gson;

public class RaftState {

    public int term;
    public String id;
    public Map<String, Node> nodes;
    public Map<String, Node> config;

    public RaftLog log;
    public Node leader;
    public String votedFor;
    public int numberOfNodes;
    public int numberConfig;
    public String type;
    public HashMap<String, Integer> matchIndex;
    public HashMap<String, Integer> nextIndex;
    public Gson gson;
    public int version;
    public int nodesVersion;
    public String level;
    public Pipe stateMachineIn;

    public RaftState(
            String nodeId,
            Pipe stateMachineIn,
            String level
    ) {
        this.nodes = null;
        this.config = null;
        this.id = nodeId;
        this.log = new RaftLog(stateMachineIn,this);
        this.term = 0;
        this.votedFor = null;
        this.leader = null;
        this.numberOfNodes = -1;
        this.numberConfig = -1;
        this.type = "follower";
        this.gson = new Gson();
        this.version = -1;
        this.level = level;
        this.stateMachineIn = stateMachineIn;
    }

    /**
     * Initializes the matchIndex and nextIndex for leader state.
     * Should be called when a node becomes leader.
     */
    public void initializeLeaderState() {
        this.matchIndex = new HashMap<>();
        this.nextIndex = new HashMap<>();

        for (String nodeId : this.nodes.keySet()) {
            this.matchIndex.put(nodeId, -1);
            this.nextIndex.put(nodeId, this.log.getLastIdx() + 1);
        }
    }

    public void initConfig(Map<String, Node> nodes) {
        this.config = nodes;
        this.nodes = nodes;
        this.numberOfNodes = nodes.size();
        this.numberConfig = config.size();
    }

    public void startUpdate(UpdateShard msg) {
        this.nodes.putAll(msg.nodes);
        this.numberOfNodes = nodes.size();
        this.nodesVersion = msg.version;
    }
}
