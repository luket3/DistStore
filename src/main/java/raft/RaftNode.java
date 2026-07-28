package raft;
import communication.HandOff;
import communication.Pipe;
import message.Message;
import message.Update;
import java.util.Map;
import cluster.Node;

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
            String id,
            Pipe stateMachineIn,
            String level,
            Map<String,Node> configData,
            Pipe ackPipe
    ) {
        // Initialize shared state in Role base class
        raftState = new RaftState(id, stateMachineIn, level, configData, ackPipe);

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
        HandOff.writeToFile(
            raftState.level + ": " + raftState.type + " " + raftState.id + " received: " + raftState.gson.toJson(message),
            raftState.getLogFilePath()
        );

        if (message.type.equals("Update") && message.version == raftState.version + 1) {
            Update castMsg = (Update) message;
            if (castMsg.action.equals("Update") || castMsg.action.equals("Distribute")) {
                // Process a membership update that should be replicated through Raft.
                    HandOff.writeToFile(
                    "Node " + raftState.id + " " + raftState.level + " level raft recieved config update request",
                    raftState.getLogFilePath()
                );
                raftState.readNewConfig(castMsg.nodes, castMsg.version);
            }
        }

        // handle request vote, append entries and client command
        // this this.term is higher functions return
        if (message.type.equals("AppendEntries"))
            followerRole.appendEntries(message);
        else if (message.type.equals("RequestVote") && !raftState.type.equals("learner"))
            followerRole.requestVote(message);
        else if ((message.type.equals("DictMsg") || message.type.equals("NodeMsg"))
                    && !raftState.type.equals("leader"))
            followerRole.handToLeader(message);

        if (raftState.type.equals("leader")) {
            if (message.type.equals("AppendEntriesReply")) {
                leaderRole.appendEntries(message);
            } else if (message.type.equals("DictMsg") || message.type.equals("NodeMsg")){
                // Handle client command
                leaderRole.appendLogEntry(message);
            }
        } else if (raftState.type.equals("candidate")) {
            if (message.type.equals("RequestVoteReply")) {
                candidateRole.requestVote(message);
            }
        }
    }

    public void sendHeartbeat() {

        HandOff.writeToFile(
            raftState.level + ": Node " + raftState.id + " - Leader sending heartbeats.",
            raftState.getLogFilePath()
        );
        this.leaderRole.broadcastAppendEntries();
    }

    public String getRole() {
        return raftState.type;
    }

    public boolean learner() {
        return (raftState.type.equals("learner"));
    }

    public void startElection() {
        // Transition to candidate state and start election process
        HandOff.writeToFile(
            raftState.level + ": Node " + raftState.id + " - Election timeout elapsed. starting election.",
            raftState.getLogFilePath()
        );
        this.candidateRole.startElection();
    }
}