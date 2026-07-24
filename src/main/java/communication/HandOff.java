package communication;
import java.util.Collection;

import cluster.Node;

public class HandOff {
    public static void sendToNode(Node node, String message) {
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

        /**
     * Send a message to all other nodes in the cluster.
     *
     * @param message the message to broadcast
     */
    public static void broadcast(String message, Collection<Node> nodes, String id) {
        Comm comm = new Comm();
        for (Node node : nodes) {
            if (!node.id.equals(id)) {
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
