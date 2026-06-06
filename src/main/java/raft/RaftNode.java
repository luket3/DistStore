package raft;

import java.util.Map;

import cluster.Node;
import communication.Pipe;
import message.Message;

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

    /**
     * Start processing an incoming connection representing an RPC.
     *
     * @param message the RPC message to process
     */
    public void handleMessage(Message message) {

        // handle request vote, append entries and client command
        // this this.term is higher functions return
        if (message.type.equals("AppendEntries"))
            followerRole.appendEntries(message);
        else if (message.type.equals("RequestVote"))
            followerRole.requestVote(message);
        else if (message.type.equals("DictMsg") 
                    && !raftState.type.equals("leader"))
            followerRole.handToLeader(message);

        if (raftState.type.equals("leader")) {
            if (message.type.equals("AppendEntriesReply")) {
                leaderRole.appendEntries(message);
            } else if (message.type.equals("DictMsg")){
                // Handle client command
                leaderRole.processClientCommand(message);
            }
        } else if (raftState.type.equals("candidate")) {
            if (message.type.equals("RequestVoteReply")) {
                candidateRole.requestVote(message);
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