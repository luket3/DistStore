package raft;

import cluster.Node;
import communication.HandOff;

/**
 * Shared utility base for the concrete Raft role implementations.
 *
 * Role subclasses inherit the node's shared consensus state and use the
 * common {@link #sendToNode(Node, String)} helper to emit serialized RPCs.
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
        try {
            HandOff.sendToNode(node,message, raftState.getLogFilePath());
        }
        catch (Exception e) {
            HandOff.writeToFile(e.getMessage(), raftState.getLogFilePath());
        }
    }
}
