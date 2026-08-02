package raft;

import cluster.Node;
import communication.HandOff;

/**
 * Shared utility base for the concrete Raft role implementations.
 *
 * Role subclasses inherit the node's shared consensus state and use the
 * common sendToNode(Node, String) helper to emit serialized RPCs.
 */
public abstract class Role {

    /** Shared Raft consensus state for the local node. */
    protected RaftState raftState;

    /**
     * Creates a role handler for the supplied Raft state.
     *
     * @param raftState shared Raft runtime state for the local node
     */
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
        try {
            HandOff.sendToNode(node,message, raftState.getLogFilePath());
        }
        catch (Exception e) {
            HandOff.writeToFile(e.getMessage(), raftState.getLogFilePath());
        }
    }
}
