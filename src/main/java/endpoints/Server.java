package endpoints;

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
 * Process entry point for a single node in the distributed store.
 *
 * The server reads its local bootstrap configuration from the network
 * configuration file, starts the shard Raft, cluster Raft, state machine,
 * and cluster state worker threads, and forwards inbound network messages
 * into the correct internal pipe for further processing.
 */
public class Server {
    /** The listener for accepting incoming connections */
    public static Listener listener;

    /** Gson instance for JSON serialization and deserialization */
    public static Gson gson;

    /** Node identifier */
    public static String nodeId;

    /** TCP port the server listens on */
    public static int port;

    /** Communication pipe for shard Raft messages */
    public static Pipe shardRaftIn;

    /** Communication pipe for state machine messages */
    public static Pipe stateMachineIn;

    /** Communication pipe for cluster state messages */
    public static Pipe clusterStateIn;

    /** Communication pipe for cluster Raft messages */
    public static Pipe clusterRaftIn;

    /** Communication pipe for cluster Raft acknowledgments */
    public static Pipe clusterRaftAck;

    /** Communication pipe for shard Raft acknowledgments */
    public static Pipe shardRaftAck;

    /** Initial cluster configuration data */
    public static Map<String, Node> configData;

    /** Flag indicating whether this node is a learner in the Raft protocol */
    public static boolean learner;


    /**
     * Initializes the server's static runtime state from command-line arguments.
     *
     * The method records the local node identity and port, reads the cluster
     * membership seed list from the network configuration file, and creates the
     * internal communication pipes used by the Raft, state machine, and cluster
     * state components.
     *
     * @param args expected to contain the node identifier and listening port,
     *     with an optional learner flag
     */
    public static void init(String args[]) throws Exception {
        nodeId = args[0];
        port = Integer.parseInt(args[1]);
        configData = new HashMap<String,Node>();
        listener = new Listener();
        shardRaftIn = new Pipe();
        stateMachineIn = new Pipe();
        shardRaftAck = new Pipe();
        clusterRaftIn = new Pipe();
        clusterStateIn = new Pipe();
        clusterRaftAck = new Pipe();
        gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageDeserializer())
                .create();
        learner = false;
        if (args.length > 2)
            learner = Boolean.parseBoolean(args[2]);

        // Read the network configuration file and populate the configData map
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
    }

    /**
     * Accepts one inbound connection, decodes the request, and routes it to the
     * appropriate internal processing component.
     *
     * Client data operations are forwarded to the shard pipeline, cluster
     * configuration requests are sent to the cluster state worker, membership
     * updates are sent to the cluster Raft input, and Raft control messages are
     * dispatched according to their level.
     *
     * @throws Exception if the socket accept, request read, or pipe write fails
     */
    public static void handleConnection() throws Exception {

        Comm comm = new Comm(listener.listenForConnection());
        String request = comm.readString();
        Message msg = gson.fromJson(request, Message.class);
        comm.closeSocket();

        if (msg.type.equals("DictMsg")) {
            // handle client data operations by forwarding to the shard Raft input
            shardRaftIn.put(msg);
        } else if (msg.type.equals("Config")) {
            // handle cluster configuration requests by forwarding to the cluster state input
            clusterStateIn.put(msg);
        } else if (msg.type.equals("NodeMsg")) {
            // handle membership updates by forwarding to the cluster Raft input
            clusterRaftIn.put(msg);
        } else if (msg.type.equals("AppendEntries") || 
                   msg.type.equals("RequestVote") || 
                   msg.type.equals("AppendEntriesReply") || 
                   msg.type.equals("RequestVoteReply")) {
            // handle Raft control messages by dispatching based on their level
            RaftMsg raftMsg = (RaftMsg) msg;
            if (raftMsg.level.equals("Shard")) {
                shardRaftIn.put(msg);
            } else if (raftMsg.level.equals("Cluster")) {
                clusterRaftIn.put(msg);
            }
        }
    }

    /**
     * Starts the server process and its supporting worker threads.
     *
     * The main method initializes the node, launches the shard Raft, cluster
     * Raft, state machine, and cluster state services, then enters the long-lived
     * socket accept loop that dispatches every incoming message.
     *
     * @param args command-line arguments containing the node identifier, port,
     *     and an optional learner flag
     * @throws Exception if startup or runtime socket handling fails
     */
    public static void main(String[] args) throws Exception {
        Server.init(args);

        // start shard Raft instance in separate thread to handle shard-level consensus
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

        // start cluster Raft instance in separate thread to handle cluster-level consensus
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

        // start state machine in separate thread to process committed operations from shard Raft
        Thread stateMachine = new Thread(
                new StateMachine(stateMachineIn, nodeId));
        stateMachine.start();

        // start cluster state worker in separate thread to manage cluster configuration and membership
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

        /* Main server loop: listen for incoming connections and pass to correct pipelines*/
        listener.createSocket(port);
        while (true) {
            handleConnection();
        }
    }
}