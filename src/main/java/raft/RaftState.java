package raft;

import java.util.HashMap;
import java.util.Map;

import cluster.Node;
import communication.Pipe;

import com.google.gson.Gson;

public class RaftState {

    public int term;
    public String id;
    public Map<String, Node> nodes;
    public RaftLog log;
    public Node leader;
    public String votedFor;
    public int numberOfNodes;
    public String type;
    public HashMap<String, Integer> matchIndex;
    public HashMap<String, Integer> nextIndex;
    public Gson gson;

    public RaftState(
            Map<String, Node> clusterNodes,
            String nodeId,
            Pipe stateMachineIn
    ) {
        this.nodes = clusterNodes;
        this.id = nodeId;
        this.log = new RaftLog(stateMachineIn);
        this.term = 0;
        this.votedFor = null;
        this.leader = null;
        this.numberOfNodes = clusterNodes.size();
        this.type = "follower";
        this.gson = new Gson();
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
}
