package raft;

import cluster.Node;
import communication.Comm;

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
        System.out.println("Sending message to node " + 
                            node.id + ": " + 
                            message);
        try {
             Comm comm = new Comm();
             comm.createSocket(node.ip, node.port);
             comm.sendString(message);
             comm.closeSocket();
        } catch (Exception e) {
            System.err.println("Failed to send message to node "
                    + node.id);
        }
    }
}
