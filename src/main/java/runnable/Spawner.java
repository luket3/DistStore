package runnable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import communication.Comm;
import communication.HandOff;
import communication.Listener;
import message.Message;
import message.MessageDeserializer;
import message.NodeMsg;
import message.Ack;
import message.Snapshot;
import cluster.Node;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Bootstrap helper that launches server processes for the distributed store.
 *
 * The spawner reads the network configuration file, works out the current
 * cluster membership (the seed nodes, updated by the newest cluster snapshot
 * on disk if one exists), starts one JVM process per member, and listens for node-management requests that add or remove
 * cluster members. Each request is acknowledged with an Ack message carrying the
 * operation result and the affected Node reference when needed.
 *
 * Add and Remove are membership changes: after the spawner answers, the client
 * asks the cluster to add or drop the node. Kill and Revive only stop or
 * restart a member's process, leaving the membership unchanged, which
 * simulates a crash and a recovery.
 */
public class Spawner {
    /** Mapping of active node threads keyed by node identifier. */
    public static HashMap<String, Thread> nodes;

    /**
     * Every node this spawner has launched and not removed, running or
     * killed, keyed by node identifier. Kept so a killed node can be revived
     * on its original port.
     */
    public static HashMap<String, Node> members;

    /** Identifiers of the seed nodes; these start as voters rather than learners. */
    public static HashMap<String, Boolean> seedIds;

    /** How long a Kill waits for the node's process to exit. */
    public static final long KILL_WAIT_MS = 10000;

    /** Listener for accepting incoming connections */
    public static Listener listener;

    /** Next available port for launching new nodes */
    public static int nextPort;

    /** Port on which the spawner listens for node-management requests */
    public static int listeningPort;

    /** Gson instance for JSON serialization and deserialization */
    public static Gson gson;

    /** Directory the nodes write their Raft snapshots to. */
    public static final String SNAPSHOT_DIR = "snapshots";

    /**
     * Initializes the spawner's runtime state and launches the cluster.
     *
     * The seed nodes and the spawner's own port come from the network
     * configuration file. If any node has written a cluster snapshot, the
     * newest one decides which nodes are members and on which ports, so the
     * cluster comes back as it was rather than as the bootstrap seeds.
     *
     * @param fresh when true, delete existing snapshots and start from the seeds only
     * @throws IOException if the configuration file cannot be read
     */
    public static void init(boolean fresh) throws IOException {
        nodes = new HashMap<>();
        members = new HashMap<>();
        seedIds = new HashMap<>();
        listener = new Listener();
        gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageDeserializer())
                .create();
        nextPort = 4567;

        // Read the seed nodes and the spawner's port from the configuration file
        List<Node> seeds = new ArrayList<>();
        try {
            List<String> configDataString =
                Files.readAllLines(Paths.get("network.config"));

            for (String line : configDataString) {
                if (line.isBlank())
                    continue;
                String[] split = line.trim().split(",");
                if (split[0].equals("spawner"))
                    listeningPort = Integer.parseInt(split[2]);
                else
                    seeds.add(new Node(split[0], split[1], Integer.parseInt(split[2])));
            }
        } catch (Exception e) {
            System.out.println("Error reading network configuration: " + e);
            System.exit(1);
        }

        if (fresh)
            deleteSnapshots();

        // Work out the membership to launch, then start every member on its recorded port
        Map<String, Node> membership = currentMembership(seeds);
        for (Node seed : seeds)
            seedIds.put(seed.id, true);

        for (Node n : membership.values()) {
            createThread(n, isLearner(n.id));
            nextPort = Math.max(nextPort, n.port + 1);
        }
    }

    /**
     * Returns the cluster membership to launch.
     *
     * Starts from the seed nodes and replays the membership changes stored in
     * the newest cluster snapshot (the one covering the highest log index), so
     * nodes added or removed before the snapshot are restored.
     *
     * @param seeds seed nodes from the network configuration file
     * @return members to launch, keyed by node id, in the order they joined
     */
    public static Map<String, Node> currentMembership(List<Node> seeds) {
        Map<String, Node> members = new LinkedHashMap<>();
        for (Node seed : seeds)
            members.put(seed.id, seed);

        Snapshot latest = newestClusterSnapshot();
        if (latest == null) {
            System.out.println("No cluster snapshot found, starting seed nodes from network.config");
            return members;
        }

        if (latest.membershipOps != null) {
            for (Snapshot.MembershipOp op : latest.membershipOps) {
                if ("Add".equals(op.action))
                    members.put(op.node.id, new Node(op.node.id, op.node.ip, op.node.port));
                else if ("Remove".equals(op.action))
                    members.remove(op.node.id);
            }
        }
        System.out.println("Restoring " + members.size() + " nodes from cluster snapshot up to index "
            + latest.lastIncludedIndex);
        return members;
    }

    /**
     * Finds the cluster snapshot covering the highest log index among all
     * nodes' snapshot files.
     *
     * @return the newest cluster snapshot, or null when none can be read
     */
    public static Snapshot newestClusterSnapshot() {
        Path dir = Paths.get(SNAPSHOT_DIR);
        if (!Files.isDirectory(dir))
            return null;

        Snapshot newest = null;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*-Cluster.json")) {
            for (Path file : files) {
                try {
                    Snapshot snap = gson.fromJson(Files.readString(file, StandardCharsets.UTF_8), Snapshot.class);
                    if (snap != null && (newest == null || snap.lastIncludedIndex > newest.lastIncludedIndex))
                        newest = snap;
                } catch (Exception e) {
                    System.out.println("Skipping unreadable snapshot " + file + ": " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading snapshot directory: " + e.getMessage());
        }
        return newest;
    }

    /**
     * Whether a node should be launched in learner mode.
     *
     * Non-seed nodes start as learners. A node that is already a voter is
     * promoted again once it restores its snapshot or replays the committed
     * configuration entries listing it as one, so restarting it as a learner
     * is safe.
     *
     * @param nodeID identifier of the node being launched
     * @return true unless the node is a seed
     */
    public static boolean isLearner(String nodeID) {
        return seedIds == null || !seedIds.containsKey(nodeID);
    }

    /**
     * Deletes every snapshot file so the nodes start with empty state.
     */
    public static void deleteSnapshots() {
        Path dir = Paths.get(SNAPSHOT_DIR);
        if (!Files.isDirectory(dir))
            return;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.json")) {
            for (Path file : files)
                Files.deleteIfExists(file);
            System.out.println("Deleted existing snapshots, starting fresh");
        } catch (IOException e) {
            System.out.println("Error deleting snapshots: " + e.getMessage());
        }
    }

    /**
     * Creates and starts a server process for the supplied node identifier.
     *
     * @param nodeID logical identifier for the server process to start
     * @param learner whether the new node should be started in learner mode
     * @return the Node object representing the newly launched process
     * @throws IOException if the process launch command cannot be assembled
     */
    public static Node createThread(String nodeID, boolean learner) throws IOException {
        // launch the node on the next port that is free and not held by a killed member
        while (!portFree(nextPort))
            nextPort++;
        Node n = new Node(nodeID, "localhost", nextPort);
        nextPort++;
        return createThread(n, learner);
    }

    /**
     * Whether a port can be given to a new node: nothing on this machine is
     * listening on it and no member (running or killed) has it.
     *
     * @param port candidate port
     * @return true if the port is free
     */
    public static boolean portFree(int port) {
        if (members != null)
            for (Node m : members.values())
                if (m.port == port)
                    return false;
        try (java.net.ServerSocket probe = new java.net.ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Creates and starts a server process for a node on the node's own port.
     *
     * @param n node to launch
     * @param learner whether the new node should be started in learner mode
     * @return the supplied node
     * @throws IOException if the process launch command cannot be assembled
     */
    public static Node createThread(Node n, boolean learner) throws IOException {
        String nodeID = n.id;
        String command = createCommand(nodeID, String.valueOf(n.port), learner);

        // Start a new thread to run the command, which launches the server process for the node.
        System.out.println("Creating new Node id:" + n.id + ", ip:" + n.ip + ", port:" + n.port);
        Thread t = new Thread(() -> runCommand(command));
        nodes.put(nodeID, t);
        if (members != null)
            members.put(nodeID, n);
        t.start();
        return n;
    }

    /**
     * Builds the JVM command used to launch a server instance for a node.
     *
     * @param nodeID identifier of the node to launch
     * @param port port assigned to that node instance
     * @param learner whether the launched server should start in learner mode
     * @return command line string used to launch the node process
     * @throws IOException if command construction fails
     */
    public static String createCommand(String nodeID, String port, boolean learner) throws IOException {
        // pass on Raft tuning set on the spawner (e.g. -Draft.heartbeatMs=50) to every node
        StringBuilder props = new StringBuilder();
        for (String name : new java.util.TreeSet<>(System.getProperties().stringPropertyNames()))
            if (name.startsWith("raft.") && !System.getProperty(name).contains(" "))
                props.append("-D").append(name).append("=").append(System.getProperty(name)).append(" ");
        return "java " + props + "-jar target/DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar " +
                                  nodeID + " " + port + " " + String.valueOf(learner);
    }

    /**
     * Executes a shell-free JVM launch command using ProcessBuilder.
     *
     * @param command serialized command line that starts a node server process
     */
    public static void runCommand(String command) {
        // Use ProcessBuilder to start a new JVM process for the node, without using a shell.
        Process p = null;
        try {
            // Split command into tokens for ProcessBuilder
            String[] tokens = command.split(" ");
            ProcessBuilder pb = new ProcessBuilder(tokens);
            pb.inheritIO(); // Show output in this terminal
            p = pb.start();
            p.waitFor();
        // If the process is interrupted, forcibly destroy it to clean up resources.
        } catch (InterruptedException e) {
            // kill the node and wait so its port is free before anything relaunches it
            p.descendants().forEach(ProcessHandle::destroyForcibly);
            p.destroyForcibly();
            try {
                p.waitFor();
            } catch (InterruptedException ignored) {
            }
        // Catch any other exceptions that occur during process launch and print the stack trace for debugging.
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends an acknowledgement back to the requesting client.
     *
     * @param client destination client to receive the acknowledgement
     * @param success whether the requested node-management operation succeeded
     * @param message descriptive status message for the caller
     * @param n reference node returned for successful creation or removal
     * @throws Exception if the acknowledgement cannot be delivered
     */
    public static void reply(Node client, boolean success, String message, Node n) throws Exception {
        Ack response = new Ack(success, message, n);
        HandOff.sendToNode(client, gson.toJson(response), null);
    }

    /**
     * Accepts one node-management request and dispatches it to the Add,
     * Remove, Kill or Revive workflow.
     *
     * Add launches a new node and Remove kills one and forgets it; the client
     * follows both with a membership change. Kill stops a member's process and
     * Revive restarts it on its original port, without any membership change.
     *
     * @throws Exception if the incoming request cannot be read or answered
     */
    public static void handleConnection() throws Exception {
        // Accept an incoming connection and read the request message.
        Comm comm = new Comm(listener.listenForConnection());
        String request = comm.readString();
        Message msg = gson.fromJson(request, Message.class);
        if (!msg.type.equals("NodeMsg")) {
            System.out.println("Invalid message type unable to handle");
            return;
        }
        NodeMsg nodeMsg = (NodeMsg) msg;
        Node n = nodeMsg.node;
        if (members == null)
            members = new HashMap<>();

        // Validate the incoming NodeMsg payload and reject malformed or duplicate requests.
        if (n == null || n.id == null) {
            reply(nodeMsg.client, false, "Invalid request: indescipherable nodeID", null);
            return;
        }

        switch (nodeMsg.action) {
            case "Add":
                handleAdd(nodeMsg, n);
                break;
            case "Remove":
                handleRemove(nodeMsg, n);
                break;
            case "Kill":
                handleKill(nodeMsg, n);
                break;
            case "Revive":
                handleRevive(nodeMsg, n);
                break;
            default:
                reply(nodeMsg.client, false, "Invalid request: unknown action " + nodeMsg.action, null);
        }
    }

    /**
     * Launches a new node. The client then asks the cluster to add it.
     *
     * @param nodeMsg the request, used to reply to the client
     * @param n the requested node; only its id is used
     * @throws Exception if the node cannot be launched or the reply cannot be sent
     */
    private static void handleAdd(NodeMsg nodeMsg, Node n) throws Exception {
        if (isRunning(n.id)) {
            reply(nodeMsg.client, false, "Illegal nodeID: nodeID already exists", null);
            return;
        }
        if (members.containsKey(n.id)) {
            reply(nodeMsg.client, false, "Illegal nodeID: " + n.id + " is a killed member, use Revive", null);
            return;
        }
        Node newNode = createThread(n.id, true);
        reply(nodeMsg.client, true, "Successfully created Node: " + n.id, newNode);
    }

    /**
     * Kills a node's process, if it is still running, and forgets it. The
     * client then asks the cluster to remove it.
     *
     * @param nodeMsg the request, used to reply to the client
     * @param n the node to remove
     * @throws Exception if the reply cannot be sent
     */
    private static void handleRemove(NodeMsg nodeMsg, Node n) throws Exception {
        if (!isRunning(n.id) && !members.containsKey(n.id)) {
            reply(nodeMsg.client, false, "removal failed: nodeID " + n.id + " not found", null);
            return;
        }
        if (isRunning(n.id)) {
            System.out.println("Killing node id:" + n.id);
            stopProcess(n.id);
        }
        members.remove(n.id);
        reply(nodeMsg.client, true, "Successfully killed Node: " + n.id, new Node(n.id, n.ip, n.port));
    }

    /**
     * Kills a node's process without changing the cluster membership, as if
     * the node had crashed. The node can be restarted with Revive.
     *
     * @param nodeMsg the request, used to reply to the client
     * @param n the node to kill
     * @throws Exception if the reply cannot be sent
     */
    private static void handleKill(NodeMsg nodeMsg, Node n) throws Exception {
        if (!isRunning(n.id)) {
            String why = members.containsKey(n.id) ? " is already killed" : " not found";
            reply(nodeMsg.client, false, "kill failed: nodeID " + n.id + why, null);
            return;
        }
        System.out.println("Killing node id:" + n.id + " (membership unchanged)");
        stopProcess(n.id);
        reply(nodeMsg.client, true, "Killed Node: " + n.id + " (still a cluster member)", members.get(n.id));
    }

    /**
     * Restarts a killed node on its original port without changing the cluster
     * membership. The node restores its snapshots, if any, and the leaders
     * bring it up to date.
     *
     * @param nodeMsg the request, used to reply to the client
     * @param n the node to revive
     * @throws Exception if the node cannot be launched or the reply cannot be sent
     */
    private static void handleRevive(NodeMsg nodeMsg, Node n) throws Exception {
        if (isRunning(n.id)) {
            reply(nodeMsg.client, false, "revive failed: nodeID " + n.id + " is already running", null);
            return;
        }
        Node member = members.get(n.id);
        if (member == null) {
            reply(nodeMsg.client, false, "revive failed: nodeID " + n.id + " was never started or has been removed", null);
            return;
        }
        System.out.println("Reviving node id:" + n.id);
        createThread(member, isLearner(member.id));
        reply(nodeMsg.client, true, "Revived Node: " + n.id, member);
    }

    /**
     * Whether the spawner has a live process for a node. A node whose process
     * exited by itself (e.g. it crashed) is forgotten here, so it counts as
     * killed and can be revived.
     *
     * @param nodeID identifier of the node
     * @return true if the node has a process thread that has not finished
     */
    private static boolean isRunning(String nodeID) {
        Thread t = nodes.get(nodeID);
        if (t != null && t.getState() == Thread.State.TERMINATED) {
            nodes.remove(nodeID);
            return false;
        }
        return t != null;
    }

    /**
     * Stops a node's process and waits for it to exit, so its port is free.
     *
     * @param nodeID identifier of the node to stop
     * @throws InterruptedException if interrupted while waiting
     */
    private static void stopProcess(String nodeID) throws InterruptedException {
        Thread t = nodes.remove(nodeID);
        if (t == null)
            return;
        t.interrupt();
        t.join(KILL_WAIT_MS);
    }

    /**
     * Starts the spawner listener and processes node-management requests until
     * the process terminates.
     *
     * @param args pass {@code --fresh} to delete existing snapshots and start
     *     from the seed nodes only
     * @throws Exception if spawner initialization or socket handling fails
     */
    public static void main(String[] args) throws Exception {
        // Initialize the spawner's runtime state and launch the cluster
        boolean fresh = args.length > 0 && args[0].equals("--fresh");
        init(fresh);
        listener.createSocket(listeningPort);

        // Enter the long-lived loop that accepts incoming node-management requests
        // and dispatches them to the appropriate Add or Remove workflow.
        while(true) {
            handleConnection();
        }
    }
}