package endpoints;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Iterator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import cluster.ConsistentHashMap;
import cluster.Node;
import communication.Comm;
import communication.HandOff;
import communication.Pipe;
import communication.Listener;
import message.DictMsg;
import message.Message;
import message.MessageDeserializer;
import message.NodeMsg;
import message.Reply;
import message.Response;
import message.Config;
import message.Ack;

/**
 * Client-side endpoint for a distributed key-value store.
 *
 * This class loads the current cluster topology from a configuration file,
 * asks a live node for the latest cluster snapshot, routes user requests to
 * the shard that owns the target key or node identity, and consumes server
 * responses through a dedicated listener thread.
 */
public class ClientImp {
    /** Map used to determine which shard holds a given key. */
    private ConsistentHashMap map;

    /* Version of the cluster configuration */
    private int version;

    /** Raw node lookup by id. */
    private Map<String, Node> nodes;

    /* JSON serializer for message conversion */
    private Gson gson;

    /* Client node information (this node) */
    private Node client;

    /* Pipe for receiving responses */
    private Pipe responsePipe;

    /* Spawner node information */
    private Node spawner;

    /* Timeout for waiting for responses in milliseconds */
    private final static int timeOut = 3000;

    /* Set of unresponsive nodes */
    private HashSet<String> dead;

    /**
     * Creates a new client endpoint bound to the supplied response port.
     *
     * @param port TCP port on which this client listens for incoming responses
     * @throws Exception if the client endpoint cannot be initialized
     */
    public ClientImp(int port) throws Exception {
        nodes = new HashMap<>();
        gson = new Gson();
        version = -1;
        client = new Node("Client0", "localhost", port);
        responsePipe = new Pipe();
        dead = new HashSet<>();
    }

    /**
     * Starts a blocking listener loop that accepts inbound server responses.
     *
     * Each decoded message is placed into the supplied response pipe so the
     * client can consume it later through the synchronous request flow.
     *
     * @param response response pipe that receives decoded server messages
     * @param port TCP port to listen on for incoming responses
     */
    public static void listen(Pipe response, int port) {
        try {
            Gson gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageDeserializer())
                .create();
            
            Listener listener = new Listener();
            listener.createSocket(port);

            while (true) {
                Comm comm = new Comm(listener.listenForConnection());
                response.put(gson.fromJson(comm.readString(), Message.class));
            }
        } catch (Exception e) {

        }
    }

    /**
     * Starts the background listener thread for the client's response port.
     */
    public void startListener() {
        Thread t = new Thread(() -> listen(responsePipe, client.port));
        t.start();
    }

    /**
     * Loads the bootstrap node list from the network configuration file.
     *
     * Each line is expected to contain one node definition in the form
     * nodeId,ip,port. The client stores all non-spawner nodes in its local
     * node registry and keeps the spawner reference separately for later
     * coordination requests.
     *
     * @throws Exception if the configuration file cannot be read or parsed
     */
    public void addSeeds() throws Exception {
        List<String> fileData =
            Files.readAllLines(Paths.get("network.config"));

        for (String line : fileData) {
            String[] split = line.split(",");
            Node n = new Node(
                    split[0],
                    split[1],
                    Integer.parseInt(split[2])
            );
            if (n.id.equals("spawner"))
                spawner = n;
            else
                nodes.put(n.id, n);
        }
    }

    /**
     * Requests and installs the latest cluster configuration from one of the
     * known nodes.
     *
     * The method repeatedly sends a Config request until a valid configuration
     * response is received or all candidate nodes have been exhausted. On
     * success, it replaces the local cluster view and version metadata with the
     * snapshot returned by the server.
     */
    public void getCluster() {
        System.out.println("Requesting cluster configuration");
        Config configMsg = new Config(client);
        Message response = null;

        // Iterate through known nodes until a valid response is received
        Iterator<Node> it = nodes.values().iterator();
        while (response == null && it.hasNext()) {
            Node n = it.next();
            if (dead.contains(n.id))
                continue;

            try {
                HandOff.sendToNode(n, gson.toJson(configMsg), null);
                response = responsePipe.take(timeOut);
            } catch (Exception e) {
                System.out.println("Error installing client map: " + e);
            }
        }

        // Install the received configuration if valid, otherwise report an error
        if (response != null && response.type.equals("Config")) {
            Config configResponse = (Config) response;
            
            map = configResponse.config;
            nodes = map.getAllNodes();
            version = configResponse.version;
            dead.clear();
        } else {
            System.out.println("Error installing client map from response");
            return;
        }

        // Print the installed cluster configuration and node lookup map
        System.out.println("Client installed with cluster configuration:");
        map.print();
        System.out.println("Node lookup map:");
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            System.out.println("  " + entry.getKey() + " -> " + entry.getValue().ip + ":" + entry.getValue().port);
        }
    }

    /**
     * Routes a prepared message to the shard that should handle it.
     *
     * DictMsg requests are resolved by key ownership, while NodeMsg requests
     * are resolved by the referenced node identity. If the destination node is
     * unreachable, the client records that node as dead and retries the next
     * available candidate.
     *
     * @param msg request message to send to the appropriate shard endpoint
     * @throws Exception if the network send fails unexpectedly
     */
    public void sendQuery(Message msg) throws Exception {
        // Determine the target node based on message type and shard ownership
        Node n = null;
        if (msg.type.equals("NodeMsg")) {
            NodeMsg castMsg = (NodeMsg) msg;
            n = map.getShard(castMsg.node.id).get(dead);
        } else if (msg.type.equals("DictMsg")) {
            DictMsg castMsg = (DictMsg) msg;
            n = map.getShard(castMsg.key).get(dead);
        }

        // Send the message to the determined node, or mark it as dead if unreachable
        try {
            HandOff.sendToNode(n, gson.toJson(msg), null);
        } catch (Exception e) {
            dead.add(n.id);
            System.err.println(e);
            if (dead.size() == nodes.size()) {
                System.out.println("no nodes responding, clearing dead nodes");
                dead.clear();
            }
        }
    }

    /**
     * Converts a command-line style query into the message type expected by the
     * distributed system.
     *
     * Supported input forms are Get key, Delete key, Put key value, Add node,
     * and Remove node. Unsupported commands return null.
     *
     * @param query user request expressed as a plain string
     * @return the corresponding DictMsg or NodeMsg instance, or null for an
     *     unsupported query
     * @throws Exception if the query cannot be parsed into a valid message
     */
    public Message buildMessage(String query) throws Exception {
        String[] split = query.split(" ");
        if (
            (split.length == 2 && (split[0].equals("Get") || split[0].equals("Delete"))) ||
            (split.length == 3 && split[0].equals("Put"))
        ) {
            DictMsg msg = new DictMsg(split[0], split[1], split.length == 3 ? split[2] : null, version, client);
            return msg;
        }
        else if (split.length == 2 && (split[0].equals("Add") || split[0].equals("Remove"))) {
            NodeMsg msg = new NodeMsg(split[0], new Node(split[1], "localhost", -1), version, client);
            return msg;
        }
        else {
            return null;
        }
    }

    /**
     * Attaches the current cluster version to a reply message before it is sent
     * back through the client workflow.
     *
     * @param msg reply message that should carry the latest cluster version
     * @return the same message instance with its version field updated, or null
     *     when the message type is not a supported reply form
     */
    public Message updateMessage(Message msg) {
        if (!(msg.type.equals("NodeMsg") || msg.type.equals("DictMsg")))
            return null;

        Reply castMsg = (Reply) msg;
        castMsg.version = this.version;
        return castMsg;
    }

    /**
     * Submits a node-management request to the spawner and waits for an
     * acknowledgement.
     *
     * A successful acknowledgement updates the message with the assigned node
     * information. A failure acknowledgement is reported back to the caller as
     * null.
     *
     * @param msg node-management request sent to the spawner
     * @return the updated message when the spawner acknowledges success, or null
     *     on failure
     * @throws Exception if the outbound send or response wait fails
     */
    public NodeMsg querySpawner(NodeMsg msg) throws Exception {
        HandOff.sendToNode(spawner, gson.toJson(msg), null);
        Message response = responsePipe.take();

        if (!response.type.equals("Ack")) {
            System.out.println("Invalid response from spawner");
            return null;
        }
        Ack ack = (Ack) response;
        dead.add(msg.node.id);

        System.out.println(ack.message);
        if (ack.success) {
            msg.node = ack.node;
            return msg;
        } else {
            return null;
        }
    }

    /**
     * Waits for the next Response message and returns its textual payload.
     *
     * The method times out after the configured client timeout and returns null
     * if no suitable response is received in time.
     *
     * @return response text from the server, or null when no response arrives
     *     before the timeout
     * @throws Exception if the response pipe cannot be read safely
     */
    public String getResponse() throws Exception{
        System.out.println("waiting for response...");
        Message msg = responsePipe.take(timeOut);

        if (msg == null)
            return null;
        else if (!msg.type.equals("Response")) {
            System.out.println("Invalid response");
        } 
        Response response = (Response) msg;
        return response.msg;
    }
}