package raft;

import cluster.Node;
import communication.HandOff;

/**
 * Base class for Raft node roles (Follower, Leader, Candidate).
 */
public abstract class Role {

    RaftState raftState;

    public Role(RaftState raftState) {
        this.raftState = raftState;
    }

    /**
     * Send a single message to the specified node.
     *
     * @param node target node
     * @param message message to send
     */
    protected void sendToNode(Node node, String message) {
        HandOff.sendToNode(node,message, raftState.getLogFilePath());
    }
}
