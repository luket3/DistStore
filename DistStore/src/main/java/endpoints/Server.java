package endpoints;

/*
 * File: Server_run_instance.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Server runner program that initializes listening sockets
 * and spawns a Server worker thread for each incoming connection.
 */

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;

import cluster.ConsistentHashMap;
import cluster.Node;
import communication.Comm;
import communication.Listener;
import communication.MessageInfo;
import communication.Pipe;
import raft.Raft;

/**
 * Server runner program that initializes listening sockets and spawns a
 * {@link StateMachine} worker thread for each incoming connection.
 */
public class Server {
    public static Listener listener;
    public static ConsistentHashMap nodes;
    public static String nodeId;
    public static int port;
    public static Pipe raftIn;
    public static Pipe stateMachineIn;
    public static int returnCode;
    public static Thread raft;


    /**
     * Initialize static runner state from command-line arguments.
     *
     * @param args expected to contain {@code nodeId} and {@code port}
     */
    public static void init(String args[]) throws Exception {
        listener = new Listener();
        nodeId = args[0];
        port = Integer.parseInt(args[1]);
        returnCode = 0;
        nodes = new ConsistentHashMap();

        try {
            List<String> fileData =
                Files.readAllLines(Paths.get("network.config"));

            for (String line : fileData) {
                String[] split = line.split(",");
                nodes.addNode(new Node(
                        split[0],
                        split[1],
                        Integer.parseInt(split[2])
                ));

                //nodes.print();
            }
        } catch (Exception e) {
            System.out.println("Error reading network configuration: " + e);
            System.exit(1);
        }

        raftIn = new Pipe();
        stateMachineIn = new Pipe();
    }

    /**
     * Accept a connection, determine if it's a Raft or client request,
     * and route it appropriately via the pipes.
     *
     * @throws Exception on socket accept or thread creation errors
     */
    public static void handOff() throws Exception {

        Comm comm = new Comm(listener.listenForConnection());
        String request = comm.readString();
        String messageType = request.split(" ")[0];

        if (!messageType.equals("AppendEntries")
                && !messageType.equals("RequestVote")
                && !messageType.equals("ClientCommand")
                && !messageType.equals("AppendEntriesReply")
                && !messageType.equals("RequestVoteReply")
                && !messageType.equals("Kill")
                && !messageType.equals("Revive")) {
            request = "ClientCommand " + Integer.toString(returnCode) + " " + request;

            MessageInfo reply = new MessageInfo(
                    Integer.toString(returnCode) + " Reply", comm);
            stateMachineIn.put(reply);
            returnCode += 1;

        }

        raftIn.put(request);
    }

    /**
     * Main entry point for the server process.
     *
     * @param args command-line arguments: {@code nodeId} {@code port}
     * @throws Exception on initialization or runtime socket errors
     */
    public static void main(String[] args) throws Exception {
        Server.init(args);

        // start Raft instance in separate thread to handle
        // cluster communication
        raft = new Thread(new Raft(
                raftIn,
                stateMachineIn,
                nodes.getShardWithNode(nodeId).getAllNodes(),
                nodeId
        ));
        raft.start();

        // start state machine instance in separate thread to handle
        // client queries
        Thread stateMachine = new Thread(
                new StateMachine(stateMachineIn, nodeId));
        stateMachine.start();

        /*
         * Main server loop: listen for incoming connections and pass to Raft
         */
        listener.createSocket(port);
        while (true) {
            handOff();
        }
    }
}