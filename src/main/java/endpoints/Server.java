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
import java.util.Map;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import communication.Comm;
import communication.Listener;
import communication.Pipe;
import raft.Raft;
import message.Message;
import message.MessageDeserializer;
import message.RaftMsg;
import cluster.ClusterState;
import cluster.Node;

/**
 * Server runner program that initializes listening sockets and spawns a
 * {@link StateMachine} worker thread for each incoming connection.
 */
public class Server {
    public static Listener listener;
    public static String nodeId;
    public static int port;
    public static Pipe shardRaftIn;
    public static Pipe stateMachineIn;
    public static int returnCode;
    public static Gson gson;
    public static Pipe clusterStateIn;
    public static Pipe clusterRaftIn;
    public static Pipe clusterRaftAck;
    public static Pipe shardRaftAck;
    public static Map<String, Node> configData;
    public static boolean learner;


    /**
     * Initialize static runner state from command-line arguments.
     *
     * @param args expected to contain {@code nodeId} and {@code port}
     */
    public static void init(String args[]) throws Exception {
        
        nodeId = args[0];
        port = Integer.parseInt(args[1]);
        learner = false;
        if (args.length > 2)
            learner = Boolean.parseBoolean(args[2]);

        returnCode = 1;
        configData = new HashMap<String,Node>();
        listener = new Listener();
        gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageDeserializer())
                .create();

        try {
            List<String> configDataString =
                Files.readAllLines(Paths.get("network.config"));

            for (String line : configDataString) {
                String[] split = line.split(",");
                Node n = new Node(split[0], split[1], Integer.parseInt(split[2]));
                if (!n.id.equals("spawner"))
                    configData.put(split[0], n);
            }

        } catch (Exception e) {
            System.out.println("Error reading network configuration: " + e);
            System.exit(1);
        }

        shardRaftIn = new Pipe();
        stateMachineIn = new Pipe();
        shardRaftAck = new Pipe();

        clusterRaftIn = new Pipe();
        clusterStateIn = new Pipe();
        clusterRaftAck = new Pipe();
    }

    /**
     * Accept a connection, determine if it's a Raft or client request,
     * and route it appropriately via the pipes.
     *
     * @throws Exception on socket accept or thread creation errors
     */
    public static void handleConnection() throws Exception {

        Comm comm = new Comm(listener.listenForConnection());
        String request = comm.readString();
        Message msg = gson.fromJson(request, Message.class);
        comm.closeSocket();

        if (msg.type.equals("DictMsg")) {
            // if it's a client request, assign a return code and trigger state machine response
            shardRaftIn.put(msg);
        } else if (msg.type.equals("Config")) {
            // return the current cluster configuration for client queries
            clusterStateIn.put(msg);
        } else if (msg.type.equals("NodeMsg")) {
            // if it's a cluster membership update, forward to ClusterState
            clusterRaftIn.put(msg);
        } else if (msg.type.equals("AppendEntries") || 
                   msg.type.equals("RequestVote") || 
                   msg.type.equals("AppendEntriesReply") || 
                   msg.type.equals("RequestVoteReply")) {
            // if it's a Raft message, forward to the appropriate Raft instance
            RaftMsg raftMsg = (RaftMsg) msg;

            if (raftMsg.level.equals("Shard")) {
                shardRaftIn.put(msg);
            } else if (raftMsg.level.equals("Cluster")) {
                clusterRaftIn.put(msg);
            }
        }
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
        Thread shardRaft = new Thread(new Raft(
                shardRaftIn,
                stateMachineIn,
                nodeId,
                "Shard",
                configData,
                shardRaftAck,
                learner
        ));
        shardRaft.start();

        Thread clusterRaft = new Thread(new Raft(
                clusterRaftIn,
                clusterStateIn,
                nodeId,
                "Cluster",
                configData,
                clusterRaftAck,
                learner
        ));
        clusterRaft.start();

        // start state machine instance in separate thread to handle
        // client queries
        Thread stateMachine = new Thread(
                new StateMachine(stateMachineIn, nodeId));
        stateMachine.start();

        Thread clusterState = new Thread(
                new ClusterState(
                        clusterStateIn,
                        configData,
                        nodeId,
                        shardRaftIn,
                        clusterRaftIn,
                        shardRaftAck,
                        clusterRaftAck
                )
        );
        clusterState.start();

        /*
         * Main server loop: listen for incoming connections and pass to Raft
         */
        listener.createSocket(port);
        while (true) {
            handleConnection();
        }
    }
}