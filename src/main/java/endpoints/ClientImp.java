package endpoints;

/*
 * File: Client.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Client for the distributed key-value store.
 */

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
 * Client for the distributed key-value store.
 *
 * <p>This class is responsible for loading the cluster configuration into a
 * consistent-hash map, sending queries to the shard responsible for a key,
 * and receiving responses from servers via the {@code Comm} helper.</p>
 */
public class ClientImp {
    /** Map used to determine which shard holds a given key. */
    private ConsistentHashMap map;
    private int version;

    /** Raw node lookup by id. */
    private Map<String, Node> nodes;
    private Gson gson;
    private Node client;
    private Pipe responsePipe;
    private Node spawner;
    private static int timeOut = 3000;
    private HashSet<String> dead;

    /**
     * Create a new {@code Client} instance and initialize communication and
     * the consistent-hash map.
     *
     * @throws Exception if initialization of underlying components fails
     */
    public ClientImp(int port) throws Exception {
        nodes = new HashMap<>();
        gson = new Gson();
        version = -1;
        client = new Node("Client0", "localhost", port);
        responsePipe = new Pipe();
        dead = new HashSet<>();
    }

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

    public void startListener() {
        Thread t = new Thread(() -> listen(responsePipe, client.port));
        t.start();
    }

    /**
     * Read the cluster configuration from `network.config`, add each
     * defined node to the consistent-hash map, and perform sample node removals
     * to exercise shard rebalancing.
     *
     * <p>The configuration file is expected to contain one node per line in
     * the format: {@code nodeId,ip,port}.</p>
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

    public void getCluster() {
        System.out.println("Requesting cluster configuration");
        Config configMsg = new Config(client);
        Message response = null;

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

        if (response.type.equals("Config") && response != null) {
            Config configResponse = (Config) response;
            
            map = configResponse.config;
            nodes = map.getAllNodes();
            version = configResponse.version;
            dead.clear();
        } else {
            System.out.println("Error installing client map from response");
            return;
        }

        System.out.println("Client installed with cluster configuration:");
        map.print();
        System.out.println("Node lookup map:");
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            System.out.println("  " + entry.getKey() + " -> " + entry.getValue().ip + ":" + entry.getValue().port);
        }
    }

    /**
     * Validate and send a textual query to the shard responsible for the
     * provided key.
     *
     * <p>Supported query formats are:
     * <ul>
     *   <li>{@code Get key}</li>
     *   <li>{@code Delete key}</li>
     *   <li>{@code Put key value}</li>
     * </ul>
     * </p>
     *
     * @param query the query string to send
     * @return a message indicating the result of the operation
     * @throws Exception on communication errors while attempting to send
     */
    public void sendQuery(Message msg) throws Exception {
        Node n = null;
        if (msg.type.equals("NodeMsg")) {
            NodeMsg castMsg = (NodeMsg) msg;
            n = map.getShard(castMsg.node.id).get(dead);
        } else if (msg.type.equals("DictMsg")) {
            DictMsg castMsg = (DictMsg) msg;
            n = map.getShard(castMsg.key).get(dead);
        }

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

    public Message updateMessage(Message msg) {
        if (!(msg.type.equals("NodeMsg") || msg.type.equals("DictMsg")))
            return null;

        Reply castMsg = (Reply) msg;
        castMsg.version = this.version;
        return castMsg;
    }

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
     * Read a response string from the currently-open communication socket
     * and close the socket afterwards.
     *
     * @return the response string read from the server
     * @throws Exception on I/O or communication errors
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