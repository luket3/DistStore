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

import cluster.ConsistentHashMap;
import cluster.Node;
import communication.Comm;
import communication.Listener;
import communication.Pipe;
import raft.Raft;
import message.Message;
import message.MessageDeserializer;
import message.DictMsg;
import message.Reply;

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
    public static Gson gson;


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
        Message msg = gson.fromJson(request, Message.class);

        if (msg.type.equals("DictMsg")) {
            DictMsg dictMsg = (DictMsg) msg;
            if (dictMsg.reply_num == -1) {
                dictMsg.reply_num = returnCode;
                msg = dictMsg;

                Reply reply = new Reply(comm, returnCode);
                stateMachineIn.put(reply);
                returnCode += 1;
            }
        }

        raftIn.put(msg);
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