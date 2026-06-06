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
import java.util.Map;

import com.google.gson.Gson;

import java.util.HashSet;

import cluster.ConsistentHashMap;
import cluster.Node;
import communication.Comm;
import message.DictMsg;
import message.NodeMsg;

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

    /** Communication helper used to send/receive messages to nodes. */
    private Comm comm;

    /** Raw node lookup by id. */
    private Map<String, Node> nodes;

    private HashSet<String> killed;

    private Gson gson;

    /**
     * Create a new {@code Client} instance and initialize communication and
     * the consistent-hash map.
     *
     * @throws Exception if initialization of underlying components fails
     */
    public ClientImp() throws Exception {
        comm = new Comm();
        map = new ConsistentHashMap();
        nodes = new HashMap<>();
        killed = new HashSet<>();
        gson = new Gson();
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
    public void addNodes() throws Exception {
        List<String> fileData =
            Files.readAllLines(Paths.get("network.config"));

        for (String line : fileData) {
            String[] split = line.split(",");
            Node n = new Node(
                    split[0],
                    split[1],
                    Integer.parseInt(split[2])
            );
            map.addNode(n);
            nodes.put(n.id, n);
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

        // Handle Kill/Revive targeting a specific node id
        if (split.length == 2 && (split[0].equals("Kill") || split[0].equals("Revive"))) {

            NodeMsg msg = new NodeMsg(split[0], split[1]);
            if (msg.action.equals("Kill"))
                killed.add(msg.nodeId);
            else if (msg.action.equals("Revive"))
                killed.remove(msg.nodeId);

            Node target = nodes.get(msg.nodeId);
            if (target == null)
                return "Error: No node with id " + msg.nodeId;

            comm.createSocket(target.ip, target.port);
            comm.sendString(gson.toJson(msg));
            comm.closeSocket();

            return msg.action + " query sent successfully to node " + msg.nodeId;
            
        }
        // Existing KV operations: Get/Delete key or Put key value
        else if (
            (split.length == 2
             && (split[0].equals("Get") || split[0].equals("Delete")))
            || (split.length == 3 && split[0].equals("Put"))
        ) {
            Node n = map.getShard(split[1]).get(killed);
            DictMsg msg = new DictMsg(split[0], split[1], split.length == 3 ? split[2] : null);

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