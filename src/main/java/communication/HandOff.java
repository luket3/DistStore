package communication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Collection;

import cluster.Node;

public class HandOff {
    public static void writeToFile(String content, String filePath) {
        try {
            Path path = Paths.get(filePath);
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            String timestamp = Instant.now().toString();
            Files.writeString(
                path,
                timestamp + " " + content + System.lineSeparator(),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );
        } catch (Exception e) {
            System.err.println("Failed to write to log file " + filePath + ": " + e.getMessage());
        }
    }

    public static void sendToNode(Node node, String message, String outputPath) {
        String output = "Sending message to node " + 
                                node.id + ": " + 
                                message;
        if (outputPath == null) {
            System.out.println(output);
        } else
            writeToFile(output, outputPath);

        try {
             Comm comm = new Comm();
             comm.createSocket(node.ip, node.port);
             comm.sendString(message);
             comm.closeSocket();
        } catch (Exception e) {
            throw new RuntimeException("failed to send message to node " + node.id);
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
