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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import communication.Comm;
import communication.Listener;
import communication.Pipe;
import raft.Raft;
import message.Message;
import message.MessageDeserializer;
import message.DictMsg;
import message.Reply;
import message.RaftMsg;
import cluster.ClusterState;

/**
 * Server runner program that initializes listening sockets and spawns a
 * {@link StateMachine} worker thread for each incoming connection.
 */
public class Server {
    public static Listener listener;
    public static String nodeId;
    public static int port;
    public static Pipe ShardRaftIn;
    public static Pipe stateMachineIn;
    public static int returnCode;
    public static Gson gson;
    public static Pipe ClusterStateIn;
    public static Pipe ClusterRaftIn;
    public static List<String> configData;


    /**
     * Initialize static runner state from command-line arguments.
     *
     * @param args expected to contain {@code nodeId} and {@code port}
     */
    public static void init(String args[]) throws Exception {
        
        gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageDeserializer())
                .create();

        listener = new Listener();
        nodeId = args[0];
        port = Integer.parseInt(args[1]);
        returnCode = 0;

        try {
            configData =
                Files.readAllLines(Paths.get("network.config"));
        } catch (Exception e) {
            System.out.println("Error reading network configuration: " + e);
            System.exit(1);
        }

        ShardRaftIn = new Pipe();
        stateMachineIn = new Pipe();

        ClusterRaftIn = new Pipe();
        ClusterStateIn = new Pipe();
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

        if (msg.type.equals("DictMsg")) {
            // if it's a client request, assign a return code and trigger state machine response
            DictMsg dictMsg = (DictMsg) msg;
            if (dictMsg.reply_num == -1) {
                dictMsg.reply_num = returnCode;
                msg = dictMsg;

                Reply reply = new Reply(comm, returnCode);
                stateMachineIn.put(reply);
                returnCode += 1;
            }
            ShardRaftIn.put(msg);
        } else if (msg.type.equals("Config")) {
            // return the current cluster configuration for client queries
            ClusterStateIn.put(new message.Reply(comm, msg.version));
            return;
        } else if (msg.type.equals("NodeMsg")) {
            // if it's a cluster membership update, forward to ClusterState
            ClusterRaftIn.put(msg);
            return;
        } else if (msg.type.equals("AppendEntries") || 
                   msg.type.equals("RequestVote") || 
                   msg.type.equals("AppendEntriesReply") || 
                   msg.type.equals("RequestVoteReply")) {
            // if it's a Raft message, forward to the appropriate Raft instance
            RaftMsg raftMsg = (RaftMsg) msg;

            if (raftMsg.level.equals("Shard")) {
                ShardRaftIn.put(msg);
            } else if (raftMsg.level.equals("Cluster")) {
                ClusterRaftIn.put(msg);
            }
            return;
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
        Thread ShardRaft = new Thread(new Raft(
                ShardRaftIn,
                stateMachineIn,
                nodeId,
                "Shard"
        ));
        ShardRaft.start();

        Thread ClusterRaft = new Thread(new Raft(
                ClusterRaftIn,
                ClusterStateIn,
                nodeId,
                "Cluster"
        ));
        ClusterRaft.start();

        // start state machine instance in separate thread to handle
        // client queries
        Thread stateMachine = new Thread(
                new StateMachine(stateMachineIn, nodeId));
        stateMachine.start();

        Thread clusterState = new Thread(
                new ClusterState(
                        ClusterStateIn,
                        configData,
                        nodeId,
                        ShardRaftIn,
                        ClusterRaftIn
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