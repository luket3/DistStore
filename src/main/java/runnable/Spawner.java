package runnable;

/*
 * File: StartNodes.java
 * Project: Distributed KV Store
 * Author: Luke
 * Date: 2026-05-22
 * Description: Helper to launch multiple server nodes by reading network.config
 *              and spawning threads to run each server instance.
 */

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import communication.Comm;
import communication.HandOff;
import communication.Listener;
import message.Message;
import message.MessageDeserializer;
import message.NodeMsg;
import message.Ack;
import cluster.Node;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Spawner {

    public static HashMap<String, Thread> nodes;
    public static Listener listener;
    public static int nextPort;
    public static Gson gson;

    // MUST be less than 6
    public static int initalSize = 3;

    public static void init() throws IOException {
        nodes = new HashMap<>();
        listener = new Listener();
        gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageDeserializer())
                .create();;
        nextPort = 4567;

        try {
            List<String> configDataString =
                Files.readAllLines(Paths.get("network.config"));

            for (String line : configDataString) {
                String[] split = line.split(",");
                nextPort = Integer.parseInt(split[2]);
                createThread(split[0], false);
            }

        } catch (Exception e) {
            System.out.println("Error reading network configuration: " + e);
            System.exit(1);
        }
    }

    public static Node createThread(String nodeID, boolean learner) throws IOException {
        String port = String.valueOf(nextPort);
        Node n = new Node(nodeID, "localhost", Integer.parseInt(port));
        nextPort++;
        String command = createCommand(nodeID, port, learner);

        System.out.println("Creating new Node id:" + n.id + ", ip:" + n.ip + ", port:" + n.port);
        Thread t = new Thread(() -> runCommand(command));
        nodes.put(nodeID, t);
        t.start();

        return n;
    }

    /**
     * Reads the network configuration file and creates a command line for each node.
     *
     * @return List of command strings, each suitable for launching a server instance.
     *         Each command has the form: "java Server <nodeId> <port>"
     */
    public static String createCommand(String nodeID, String port, boolean learner) throws IOException {
        return "java -jar target/DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar " +  
                                  nodeID + " " + port + " " + String.valueOf(learner);
    }

    /**
     * Executes a given command using ProcessBuilder.
     *
     * @param command The command string to execute.
     */
    public static void runCommand(String command) {
        Process p = null;
        try {
            // Split command into tokens for ProcessBuilder
            String[] tokens = command.split(" ");

            ProcessBuilder pb = new ProcessBuilder(tokens);
            pb.inheritIO(); // Show output in this terminal
            p = pb.start();
            p.waitFor();
        } catch (InterruptedException e) {
                p.destroyForcibly();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void reply(Node client, boolean success, String message, Node n) throws Exception {
        Ack response = new Ack(success, message, n);
        HandOff.sendToNode(client, gson.toJson(response), null);
    }

    public static void handleConnection() throws Exception {
        Comm comm = new Comm(listener.listenForConnection());
        String request = comm.readString();
        Message msg = gson.fromJson(request, Message.class);

        if (!msg.type.equals("NodeMsg")) {
            System.out.println("Invalid message type unable to handle");
            return;
        }

        NodeMsg nodeMsg = (NodeMsg) msg;
        Node n = nodeMsg.node;
        if (n == null || n.id == null) {
            reply(nodeMsg.client, false, "Invalid request: indescipherable nodeID", null);
            return;
        }

        if (nodeMsg.action.equals("Add")) {
            if (nodes.get(n.id) != null) {
                reply(nodeMsg.client, false, "Illegal nodeID: nodeID already exists", null);
                return;
            }
            else if (n.id.startsWith("SEED")) {
                reply(nodeMsg.client, false, "Illegal nodeID: nodeID cannot start with SEED", null);
                return;
            }

            Node newNode = createThread(n.id, true);
            reply(nodeMsg.client, true, "Successfully created Node: " + n.id, newNode);
        } else if (nodeMsg.action.equals("Remove")) {
            Thread targetNode = nodes.get(n.id);
            if (targetNode == null) {
                reply(nodeMsg.client, false, "removal failed: nodeID " + n.id + " not found", null);
                return;
            }

            System.out.println("Killing node id:" + n.id);
            targetNode.interrupt();
            nodes.remove(n.id);
            reply(nodeMsg.client, true, "Successfully killed Node: " + n.id, new Node(n.id, n.ip, n.port));
        }
    }

    public static void main(String[] args) throws Exception {
        init();

        int port = Integer.parseInt(Files.readString(Path.of("spawner.config")));
        listener.createSocket(port);

        while(true) {
            handleConnection();
        }
    }
}