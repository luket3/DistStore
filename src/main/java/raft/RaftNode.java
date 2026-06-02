package raft;

import java.util.Map;

import cluster.Node;
import communication.Pipe;

/**
 * Base class for Raft implementations holding common state.
 */
public class RaftNode {
    /** Current role type: "follower", "candidate", or "leader". */
    private Candidate candidateRole;
    private Follower followerRole;
    private Leader leaderRole;
    private RaftState raftState;
    
    /**
     * Constructor initializes common Raft state.
     */
    public RaftNode(
            Map<String, Node> clusterNodes,
            String id,
            Pipe stateMachineIn
    ) {
        // Initialize shared state in Role base class
        raftState = new RaftState(clusterNodes, id, stateMachineIn);

        // Initialize role instances
        this.candidateRole = new Candidate(raftState);
        this.followerRole = new Follower(raftState);
        this.leaderRole = new Leader(raftState);
    }

    private String[] splitMsg(String message) {

        String[] split = new String[7];
        int splitIdx = 0;
        int startIdx = 0;
        String word = "";

        for (int i = 0; i <= message.length(); i++) {
            if (i == message.length() || message.charAt(i) == ' ') {
                word = message.substring(startIdx, i);
                split[splitIdx] = word;
                splitIdx++;
                startIdx = i + 1;
            }
            else if (message.charAt(i) == '[') {
                word = message.substring(i);
                split[splitIdx] = word;
                break;
            }
        }

        return split;
    }

    /**
     * Start processing an incoming connection representing an RPC.
     *
     * @param message the RPC message to process
     */
    public void handleMessage(String message) {

        // make command seperate
        String[] parts = splitMsg(message);
        String rpcType = parts[0];

        // handle request vote, append entries and client command
        // this this.term is higher functions return
        if (rpcType.equals("AppendEntries"))
            followerRole.appendEntries(parts);
        else if (rpcType.equals("RequestVote"))
            followerRole.requestVote(parts);
        else if (rpcType.equals("ClientCommand") 
                    && !raftState.type.equals("leader"))
            followerRole.handToLeader(message);

        if (raftState.type.equals("leader")) {
            if (rpcType.equals("AppendEntriesReply")) {
                leaderRole.appendEntries(parts);
            } else if (rpcType.equals("ClientCommand")){
                // Handle client command
                leaderRole.processClientCommand(message);
            }
        } else if (raftState.type.equals("candidate")) {
            if (rpcType.equals("RequestVoteReply")) {
                candidateRole.requestVote(parts);
            }
        }
    }

    public void sendHeartbeat() {
        this.leaderRole.broadcastAppendEntries();
    }

    public String getRole() {
        return raftState.type;
    }

    public void startElection() {
        // Transition to candidate state and start election process
        this.candidateRole.startElection();
    }
}