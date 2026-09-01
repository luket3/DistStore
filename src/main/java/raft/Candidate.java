package raft;

import java.util.HashSet;
import java.util.Set;
import communication.HandOff;
import message.RequestVoteReply;
import message.RequestVote;
import cluster.Node;

/**
 * Raft role handler for a node that is campaigning for leadership.
 *
 * The candidate increments its election term, sends RequestVote RPCs to the
 * known voters, records each granted vote, and transitions into leader state
 * once it receives a strict majority of approvals.
 */
public class Candidate extends Role {

    /** Number of distinct vote responses accepted during the current election. */
    private int votesReceived;

    /** Set of node identifiers that have already been counted in this election. */
    private Set<String> votedNodes;

    /**
     * Creates a candidate role for the supplied Raft state.
     *
     * @param raftState shared Raft runtime state for the local node
     */
    public Candidate(RaftState raftState) {
        super(raftState);
        this.votesReceived = 0;
        this.votedNodes = new HashSet<>();
    }

    /**
     * Processes a single RequestVote reply while the node is campaigning.
     *
     * The method rejects stale replies from older terms, updates the local
     * Raft state when a higher term is observed, and counts a unique vote only
     * once per sender. A majority of granted votes promotes the node to leader.
     *
     * @param RVReply RequestVoteReply RPC to evaluate
     * @return true when the reply was accepted for processing, false for stale
     *     or obsolete election state
     */
    public boolean requestVote(RequestVoteReply RVReply) {
        // Update term and revert to follower if we see a higher term
        if (RVReply.term > raftState.term) {
            raftState.term = RVReply.term;
            raftState.type = "follower";
            raftState.votedFor = null;
            return false;
        }

        // check for stale term or duplicate vote from the same node
        if (RVReply.term < raftState.term) {
            return false;
        } else if (votedNodes.contains(RVReply.senderId)) {
            return false;
        }

        // Count the vote if grated and promote to leader if we have a majority
        if (RVReply.voteGranted) {
            votedNodes.add(RVReply.senderId);
            this.votesReceived++;
            if (this.votesReceived > raftState.voters.size() / 2) {
                raftState.type = "leader";
                raftState.initializeLeaderState();
            }
        }
        return true;
    }

    /**
     * Starts a new election for this node.
     *
     * The node increments its term, records a self-vote, clears any previous
     * per-election vote bookkeeping, and broadcasts a RequestVote RPC to all
     * configured voters.
     */
    public void startElection() {
        // Increment the term and transition to candidate state
        raftState.term++;
        raftState.type = "candidate";
        this.votedNodes.clear();
        raftState.votedFor = raftState.id;
        this.votesReceived = 1;
        this.votedNodes.add(raftState.id);

        // Broadcast RequestVote RPCs to the other cluster nodes.
        RequestVote RVmsg = new RequestVote(
            raftState.level,
            raftState.term,
            raftState.id,
            raftState.log.getLastIdx(),
            raftState.log.getLastTerm(),
            raftState.version
        );
        broadcast(raftState.gson.toJson(RVmsg));
    }

    /**
     * Broadcasts a serialized Raft message to every voter except the local
     * node.
     *
     * @param message serialized message payload to transmit
     */
    public void broadcast(String message) {
        HandOff.writeToFile(
            raftState.level + ": Candidate " + raftState.id + " Broadcasting message to all nodes: " + message,
            raftState.getLogFilePath()
        );

        for (Node node : raftState.voters.values()) {
            if (!node.id.equals(raftState.id))
                sendToNode(node, message);
        }
    }
}