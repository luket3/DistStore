package raft;

import communication.HandOff;
import communication.Pipe;
import message.Message;
import message.Update;
import message.Reply;
import message.Response;
import message.AppendEntries;
import message.RequestVote;
import message.RequestVoteReply;
import message.AppendEntriesReply;
import java.util.Map;
import cluster.Node;

/**
 * Dispatch layer for one Raft node.
 *
 * This class routes inbound protocol traffic to the active role handler for
 * the node, including the follower, candidate, and leader implementations,
 * while also coordinating membership updates and pending learner replication.
 */
public class RaftNode {
    /** Candidate role handler used during election processing. */
    private Candidate candidateRole;

    /** Follower role handler used for vote and append-entry processing. */
    private Follower followerRole;

    /** Leader role handler used for replication and commit coordination. */
    private Leader leaderRole;

    /** Shared Raft consensus state for this node. */
    private RaftState raftState;
    
    /**
     * Creates a Raft node dispatcher with a shared Raft state object and the
     * role handlers used during election and replication.
     *
     * @param id local node identifier
     * @param stateMachineIn output pipe used for committed client operations
     * @param level Raft pipeline level for this node
     * @param configData bootstrap node membership map
     * @param ackPipe acknowledgement pipe for configuration changes
     * @param learner whether this instance should behave as a learner
     */
    public RaftNode(
            String id,
            Pipe stateMachineIn,
            String level,
            Map<String,Node> configData,
            Pipe ackPipe,
            boolean learner
    ) {
        raftState = new RaftState(id, stateMachineIn, level, configData, ackPipe, learner);
        this.candidateRole = new Candidate(raftState);
        this.followerRole = new Follower(raftState);
        this.leaderRole = new Leader(raftState);
    }

    /**
     * Dispatches one incoming message to the role behavior currently active on
     * the node.
     *
     * Update messages are processed as membership replication requests, vote
     * and append requests are delegated to the follower role, client commands
     * are appended by the leader or forwarded to the current leader, and reply
     * messages are consumed by the active election or replication state.
     *
     * @param message inbound protocol message to handle
     */
    public void handleMessage(Message message) {
        // Log the incoming message for debugging and auditing purposes
        HandOff.writeToFile(
            raftState.level + ": " + raftState.type + " " + raftState.id + " received: " + raftState.gson.toJson(message),
            raftState.getLogFilePath()
        );

        // Handle membership update messages
        if (message.type.equals("Update")) {
            Update castMsg = (Update) message;
            if ((castMsg.action.equals("Update") || castMsg.action.equals("Distribute")) && castMsg.version == raftState.version + 1) {
                // Process a membership update that should be replicated through Raft.
                    HandOff.writeToFile(
                    "Node " + raftState.id + " " + raftState.level + " level raft recieved config update request",
                    raftState.getLogFilePath()
                );
                raftState.readNewConfig(castMsg.nodes, castMsg.version);
            }
        }

        // handle request vote, append entries, DictMsg and NodeMsg messages
        if (message.type.equals("AppendEntries"))
            followerRole.appendEntries((AppendEntries) message);
        else if (message.type.equals("RequestVote") && !raftState.type.equals("learner"))
            followerRole.requestVote((RequestVote) message);
        else if ((message.type.equals("DictMsg") || message.type.equals("NodeMsg"))) {
            Reply msg = (Reply) message;
            if (msg.version < raftState.version) {
                HandOff.sendToNode(msg.client, raftState.gson.toJson(new Response("Invalid config")), raftState.getLogFilePath());
                return;
            }

            // If this node is a follower or candidate, forward the message to the leader
            if (!raftState.type.equals("leader"))
                followerRole.handToLeader(message);
            // If this node is the leader, append the message to the log for replication
            else
                leaderRole.appendLogEntry(message);
        }

        // handle replies to RequestVote and AppendEntries messages
        if (raftState.type.equals("candidate")) {
            if (message.type.equals("RequestVoteReply")) {
                candidateRole.requestVote((RequestVoteReply) message);
            }
        } else if (raftState.type.equals("leader")) {
            if (message.type.equals("AppendEntriesReply")) {
                leaderRole.appendEntries((AppendEntriesReply) message);
            }
        }

        // Send AppendEntries where appropriate
        if (raftState.type.equals("leader") && raftState.getPendingLog()) {
            leaderRole.broadcastAppendEntries();
        }
        if (raftState.newLearners.size() > 0) {
            if (raftState.type.equals("leader")) {
                for (Node n : raftState.newLearners.values()) {
                    leaderRole.sendAppendEntries(n);
                }
            }
            raftState.newLearners.clear();
        }
    }

    /**
     * Broadcasts a heartbeat-style AppendEntries sequence on behalf of the
     * leader role.
     */
    public void sendHeartbeat() {

        HandOff.writeToFile(
            raftState.level + ": Node " + raftState.id + " - Leader sending heartbeats.",
            raftState.getLogFilePath()
        );
        this.leaderRole.broadcastAppendEntries();
    }

    /**
     * Returns the active role label for the current node.
     *
     * @return current Raft role name
     */
    public String getRole() {
        return raftState.type;
    }

    /**
     * Reports whether the node is operating in learner-only mode.
     *
     * @return true when the node is configured as a learner
     */
    public boolean learner() {
        return (raftState.type.equals("learner"));
    }

    /**
     * Starts a new election by transitioning the local node to candidate mode.
     */
    public void startElection() {
        HandOff.writeToFile(
            raftState.level + ": Node " + raftState.id + " - Election timeout elapsed. starting election.",
            raftState.getLogFilePath()
        );
        this.candidateRole.startElection();
    }
}