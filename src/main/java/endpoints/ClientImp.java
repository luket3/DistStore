package endpoints;

/*
 * File: Client.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Client for the distributed key-value store.
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.gson.Gson;

import cluster.ConsistentHashMap;
import cluster.Node;
import communication.Comm;
import message.DictMsg;
import message.NodeMsg;
import message.Config;

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

    /** Communication helper used to send/receive messages to nodes. */
    private Comm comm;

    /** Raw node lookup by id. */
    private Map<String, Node> nodes;

    private Gson gson;
    private List<Thread> threads;

    /**
     * Create a new {@code Client} instance and initialize communication and
     * the consistent-hash map.
     *
     * @throws Exception if initialization of underlying components fails
     */
    public ClientImp(List<Thread> threads) throws Exception {
        comm = new Comm();
        nodes = new HashMap<>();
        gson = new Gson();
        version = -1;
        this.threads = threads;
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
            nodes.put(n.id, n);
        }
    }

    public void initCluster() {
        // get a random node from the cluster to use as the initial point for the consistent hash map
        Node n = nodes.values()
                   .stream()
                   .skip(ThreadLocalRandom.current().nextInt(nodes.size()))
                   .findFirst()
                   .orElse(null);

        Config configMsg = new Config();

        String response = null;
        try {
            comm.createSocket(n.ip, n.port);
            String json = gson.toJson(configMsg);
            comm.sendString(json);
            response = comm.readString();
        } catch (Exception e) {
            System.out.println("Error initializing client map: " + e);
        }

        Config responseMsg = gson.fromJson(response, Config.class);
        map = responseMsg.config;
        nodes = map.getAllNodes();
        version = responseMsg.version;

        System.out.println("Client initialized with cluster configuration:");
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
    public String sendQuery(String query) throws Exception {
        String[] split = query.split(" ");

        if (
            (split.length == 2
             && (split[0].equals("Get") || split[0].equals("Delete")))
            || (split.length == 3 && split[0].equals("Put"))
        ) {
            Node n = map.getShard(split[1]).get(null);
            DictMsg msg = new DictMsg(split[0], split[1], split.length == 3 ? split[2] : null, version);

            comm.createSocket(n.ip, n.port);
            comm.sendString(gson.toJson(msg));

            return msg.type + " query sent successfully to node " + n.id;
        }
        else if (split.length == 2 && (split[0].equals("Add") || split[0].equals("Remove"))) {
            Node n = map.getShard(split[1]).get(null);

            String nextPort = Files.readString(Path.of("nextPort.config"));
            int nextPortInt = Integer.parseInt(nextPort);
            Files.writeString(Path.of("nextPort.config"), String.valueOf(nextPortInt+1));

            String command = "java -jar target/DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar " +  
                                n.id + " " + nextPort;
            Thread t = new Thread(() -> runnable.StartNodes.runCommand(command));
            threads.add(t);
            t.start();

            NodeMsg msg = new NodeMsg(split[0], split[1], version);

            comm.createSocket(n.ip, n.port);
            comm.sendString(gson.toJson(msg));

            return msg.type + " query sent successfully to node " + n.id;
        }
        else {
            return "Error: Invalid query format";
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
        String response = comm.readString();
        comm.closeSocket();
        return response;
    }
}