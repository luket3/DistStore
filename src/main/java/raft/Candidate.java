package raft;

import java.util.HashSet;
import java.util.Set;

import cluster.Node;
import communication.Comm;
import message.Message;
import message.RequestVoteReply;
import message.RequestVote;

/**
 * Candidate role for Raft consensus.
 */
public class Candidate extends Role {

    private int votesReceived;
    private Set<String> votedNodes;

    public Candidate(RaftState raftState) {
        super(raftState);
        this.votesReceived = 0;
        this.votedNodes = new HashSet<>();
    }

    public boolean requestVote(Message message) {
        // Handle a RequestVote response while this node is a candidate.
        // Format: RequestVoteReply <term> <senderID> <voteGranted>
        
        RequestVoteReply RVReply = (RequestVoteReply) message;
        System.out.println("Candidate " + raftState.id + " received: " + raftState.gson.toJson(RVReply));

        // Update term and revert to follower if we see a higher term
        if (RVReply.term > raftState.term) {
            raftState.term = RVReply.term;
            raftState.type = "follower";
            raftState.votedFor = null;
            return false;
        }

        // Ignore votes from previous terms
        if (RVReply.term < raftState.term) {
            return false;
        }

        // Only count each node's vote once
        if (votedNodes.contains(RVReply.senderId)) {
            return false;
        }

        if (RVReply.voteGranted) {
            votedNodes.add(RVReply.senderId);
            this.votesReceived++;
            // Check if we have won the election
            if (this.votesReceived > raftState.numberOfNodes / 2) {
                raftState.type = "leader";
                // Initialize leader state (matchIndex and nextIndex) for this node
                raftState.initializeLeaderState();
            }
        }
        return true;
    }

    public void startElection() {
        // Increment term and transition this node into candidate state.
        raftState.term++;
        raftState.type = "candidate";
        // Clear voting set for new election
        this.votedNodes.clear();
        // Vote for self as the current candidate.
        raftState.votedFor = raftState.id;
        // In a real implementation this would be the node's own identifier
        this.votesReceived = 1; // Vote for self
        this.votedNodes.add(raftState.id); // Record self vote

        // Broadcast RequestVote RPCs to the other cluster nodes.
        RequestVote RVmsg = new RequestVote(
            raftState.level,
            raftState.term,
            raftState.id,
            raftState.log.getLastIdx(),
            raftState.log.getLastTerm()
        );
        broadcast(raftState.gson.toJson(RVmsg));
    }

    /**
     * Send a message to all other nodes in the cluster.
     *
     * @param message the message to broadcast
     */
    public void broadcast(String message) {
        System.out.println("Broadcasting message to all nodes: " +
                            message);

        Comm comm = new Comm();
        for (Node node : raftState.nodes.values()) {
            if (!node.id.equals(raftState.id)) {
                try {
                    comm.createSocket(node.ip, node.port);
                    comm.sendString(message);
                    comm.closeSocket();
                } catch (Exception e) {
                    System.err.println("Failed to send message to node "
                            + node.id);
                }
            }
        }
    }
}