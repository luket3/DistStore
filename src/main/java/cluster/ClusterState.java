package cluster;

import communication.HandOff;
import communication.Pipe;
import message.Message;
import java.util.Map;
import java.util.HashMap;
import message.NodeMsg;
import message.Config;
import message.Response;
import com.google.gson.Gson;

/**
 * Maintains the local node's view of the cluster membership state.
 *
 * This class consumes membership updates from an input pipe, applies the
 * corresponding Add or Remove operation to a local consistent-hash model,
 * and then publishes the new cluster membership view through the cluster Raft
 * pipeline. It also answers configuration requests by returning the current
 * membership snapshot and version number to the requesting client.
 */
public class ClusterState implements Runnable {
    /** input pipe used to receive membership and configuration messages */
    private Pipe inPipe;

    /** local consistent-hash model of the cluster membership state */
    private ConsistentHashMap cluster;

    /** version number of the current cluster membership state */
    private int version;

    /** identifier of the local node */
    private String nodeID;

    /** identifier of the current shard to which the local node belongs */
    private String currShardID;

    /** current size of the shard to which the local node belongs */
    private int currShardSize;

    /** input pipe used to send shard-level Raft updates */
    private Pipe shardRaftIn;

    /** input pipe used to send cluster-level Raft updates */
    private Pipe clusterRaftIn;

    /** output pipe used to wait for shard acknowledgements */
    private Pipe shardRaftOut;

    /** output pipe used to wait for cluster acknowledgements */
    private Pipe clusterRaftOut;

    /** Gson instance for JSON serialization and deserialization */
    private Gson gson;

    /** path to the log file for recording cluster state operations */
    private String logPath;

    /**
     * Creates a new cluster state manager for the local node.
     *
     * @param inPipe input pipe used to receive membership and configuration messages
     * @param nodes initial node set used to seed the local consistent-hash view
     * @param nodeID identifier of the local node
     * @param shardRaftIn pipe used to send shard-level Raft updates
     * @param clusterRaftIn pipe used to send cluster-level Raft updates
     * @param shardRaftOut pipe used to wait for shard acknowledgements
     * @param clusterRaftOut pipe used to wait for cluster acknowledgements
     * @throws Exception if the local shard cannot be resolved from the initial node set
     */
    public ClusterState(Pipe inPipe, Map<String, Node> nodes, String nodeID, Pipe shardRaftIn, Pipe clusterRaftIn, Pipe shardRaftOut, Pipe clusterRaftOut) throws Exception {
        this.inPipe = inPipe;
        this.cluster = new ConsistentHashMap();
        this.nodeID = nodeID;
        this.shardRaftIn = shardRaftIn;
        this.clusterRaftIn = clusterRaftIn;
        this.shardRaftOut = shardRaftOut;
        this.clusterRaftOut = clusterRaftOut;
        this.version = 0;
        this.gson = new Gson();
        this.logPath = "logs/ClusterState.log";

        // Initialize the cluster with the provided configuration data.
        for (Node n : nodes.values()) {
            cluster.addNode(n);
        }

        Shard currShard = cluster.getShardWithNode(nodeID);
        this.currShardSize = -1;
        this.currShardID = null;
        if (currShard != null) {
            this.currShardID = currShard.id;
            this.currShardSize = currShard.getAllNodes().size();
        }
    }

    /**
     * Processes a single incoming message from the input pipe.
     *
     * A Config message causes this class to return the current cluster snapshot
     * to the requesting client. A NodeMsg message triggers a membership change,
     * followed by cluster and shard Raft acknowledgement waits before the
     * result is reported back to the client when one is attached.
     *
     * @throws Exception if an input or output pipe operation fails while handling
     *     the message
     */
    public void takeAndHandle() throws Exception{
        // Block until a new message arrives on the input pipe.
        Message message = inPipe.take();
        if (message == null)
            return;

        // Only Config and NodeMsg messages are recognized.
        if (message.type.equals("Config")) {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: replying with cluster config", this.logPath);
            Config configMsg = (Config) message;
            replyConfig(configMsg.client);
            return;
        }
        if (!(message.type.equals("NodeMsg"))) {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: unsupported message type " + message.type, this.logPath);
            return;
        }

        // Handle a membership change request.
        NodeMsg nodeMsg = (NodeMsg) message;
        Map<String, Shard> result = updateCluster(nodeMsg);

        if (cluster.getShardWithNode(nodeID) != null) {
            // send update message to cluster Raft pipe
            HandOff.writeToFile("Node " + this.nodeID + " sending update message to raft cluster", this.logPath);
            clusterRaftIn.put(new message.Update(cluster.getAllNodes(), version, false));

            // Update shard if necessary, and wait for acknowledgements from both the cluster and shard Raft pipelines.
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: waiting for acknowledgement from cluster for version: " + this.version, this.logPath);
            this.clusterRaftOut.take();
            if (updateShard(result)) {
                HandOff.writeToFile("Node " + this.nodeID + " ClusterState: waiting for acknowledgement from Shard for version: " + this.version, this.logPath);
                this.shardRaftOut.take();
            }

            // send response to client if one is attached to the NodeMsg request.
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: new config finalised", this.logPath);
            if (nodeMsg.client != null) {
                Response response = new Response("Cluster successfully updated");
                HandOff.sendToNode(nodeMsg.client, gson.toJson(response), this.logPath);
            }
        }

        Shard currShard = cluster.getShardWithNode(nodeID);
        if (currShard != null) {
            this.currShardID = currShard.id;
            this.currShardSize = currShard.getAllNodes().size();
        }
    }

    /**
     * Sends the current cluster snapshot and version to the requesting client.
     *
     * @param client destination node for the configuration response
     */
    public void replyConfig(Node client) {
        Config configMsg = new Config(cluster, version);

        try {
            HandOff.sendToNode(client, gson.toJson(configMsg), this.logPath);
        } catch (Exception e) {
            HandOff.writeToFile("Error sending config reply: " + e, this.logPath);
        }
    }

    /**
     * Applies a single membership change to the local consistent-hash state.
     *
     * Supported actions are Add and Remove. Add validates the node address,
     * inserts the node into the local cluster model, increments the version, and
     * publishes the updated node list to the cluster Raft input pipe. Remove
     * removes the requested node from the local view, increments the version,
     * and publishes the new cluster snapshot to the same Raft pipe.
     *
     * @param nodeMsg membership change request to apply
     * @return returns the result of the membership change, which may contain a new shard if a split occurred
     * @throws Exception if a pipe write or cluster access operation fails
     */
    private Map<String, Shard> updateCluster(NodeMsg nodeMsg) throws Exception {
        String action = nodeMsg.action;

        // Add a node only when the supplied address information is valid.
        Map<String, Shard> result = null;
        if (action.equals("Add") && nodeMsg.node.ip != null && nodeMsg.node.port != -1) {
            Node node = new Node(nodeMsg.node.id, nodeMsg.node.ip, nodeMsg.node.port);
            result = cluster.addNode(node);
            version++;
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: added node " + nodeMsg.node.id, this.logPath);
        } else if (action.equals("Remove")) {
            // Remove a node from the cluster and publish the new stable membership view.
            result = new HashMap<>();
            result.put("old", cluster.removeNode(nodeMsg.node.id));
            version++;
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: removed node " + nodeMsg.node.id, this.logPath);
        } else {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: unknown NodeMsg action " + nodeMsg.action, this.logPath);
            return null;
        }

        return result;
    }

    /**
     * Propagates shard membership information to the shard Raft pipeline.
     *
     * If the local node has moved to a different shard, or the currently
     * assigned shard has shrunk beyond the previous tracked size, the method
     * records that a shard-level redistribution is needed. When the updated
     * shard is the same as the one the local node already belongs to, this
     * method sends the affected shard's node list to the shard Raft input pipe.
     *
     * @param result the result of the last membership change, which may contain a new shard
     * @return true if a shard-level redistribution is needed, otherwise false
     * @throws Exception if writing to the shard Raft pipe fails
     */
    private boolean updateShard(Map<String, Shard> result) throws Exception {
        Shard shard = cluster.getShardWithNode(nodeID);

        // If the local node migrated to a different shard prepare it to recieve new data
        boolean shardChanged = false;
        if (result != null && result.containsKey("new") && (result.get("new").id.equals(shard.id) || result.get("old").id.equals(shard.id))) {
            HandOff.writeToFile("Node " + this.nodeID + " sending SplitShard message to Shard", this.logPath);
            shardRaftIn.put(new message.SplitShard(result.get("old").getAllNodes(), result.get("new").getAllNodes(), version));
            shardChanged = true;
        } else if (!(currShardID == null) && !this.currShardID.equals(shard.id)) {
            // check if node has moved to a different shard
            HandOff.writeToFile("Node " + this.nodeID + " sending Update message to Shard", this.logPath);
            shardRaftIn.put(new message.Update(shard.getAllNodes(), version, true));
            shardChanged = true;
        } else if (currShardID == null || (shard.getAllNodes().size() != this.currShardSize)) {
            // check if shard has shrunk or grown beyond the previous tracked size
            HandOff.writeToFile("Node " + this.nodeID + " sending Update message to Shard", this.logPath);
            shardRaftIn.put(new message.Update(shard.getAllNodes(), version, false));
            shardChanged = true;
        }

        return shardChanged;
    }

    /**
     * Runs the service loop for the cluster state component.
     *
     * The thread repeatedly waits for the next input message, processes it, and
     * logs any failure so the service can continue to accept new work.
     */
    @Override
    public void run() {
        while (true) {
            try {
                takeAndHandle();
            } catch (Exception e) {
                HandOff.writeToFile("ClusterState: " + nodeID + " error updating cluster: " + e.getMessage(), this.logPath);
            }
        }
    }
}
