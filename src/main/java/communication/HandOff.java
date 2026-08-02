package communication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

import cluster.Node;

/**
 * Utility class for handing off messages between nodes and persisting them to disk.
 * Provides methods for sending messages to nodes via network communication and
 * persisting messages to log files with timestamps.
 */
public class HandOff {

    /**
     * Writes a timestamped message to a log file, creating directories as needed.
     *
     * @param content The message content to write to the file
     * @param filePath The path to the log file where content will be appended
     */
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

    /**
     * Sends a message to a specified node via network communication and optionally
     * logs the transmission to a file.
     *
     * @param node The target node to send the message to
     * @param message The message content to send
     * @param outputPath Optional path to log file; if null, message is printed to console
     */
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
}
