package cluster;
import communication.HandOff;
import communication.Pipe;
import message.Message;

import java.util.Map;
import message.NodeMsg;
import message.Config;
import message.Response;
import com.google.gson.Gson;

/**
 * Maintains the local node's view of the cluster topology and propagates
 * cluster membership changes to the shard and cluster Raft pipes.
 *
 * <p>This class continuously listens for incoming {@link NodeMsg} messages, applies
 * add/remove operations to the local {@link ConsistentHashMap}, and then pushes the
 * resulting state updates to the relevant Raft communication channels.</p>
 */
public class ClusterState implements Runnable {

    Pipe inPipe;
    ConsistentHashMap cluster;
    String currShardID;
    int currShardSize;
    int version;
    String nodeID;
    Pipe shardRaftIn;
    Pipe clusterRaftIn;
    Pipe shardRaftOut;
    Pipe clusterRaftOut;
    Gson gson;
    String logPath;

    /**
     * Creates a new cluster state manager for the given node.
     *
     * @param inPipe pipe from which membership update messages are consumed
     * @param cluster the current cluster map used to track nodes and shards
     * @param NodeID the local node identifier
     * @param ShardRaft pipe for shard-related Raft messages
     * @param clusterRaft pipe for cluster-wide Raft messages
     * @throws Exception if the current shard cannot be resolved from the cluster map
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

        // Cache the node's initial shard identity and shard size for later comparisons.
        Shard currShard = cluster.getShard(nodeID);
        this.currShardID = currShard.id;
        this.currShardSize = currShard.size();
    }

    public void takeAndHandle() throws Exception{
        // Block until a new message arrives on the input pipe.
        Message message = inPipe.take();
        if (message == null)
            return;

        // Only NodeMsg messages are recognized for cluster topology updates.
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

        NodeMsg nodeMsg = (NodeMsg) message;
        Shard updated = updateCluster(nodeMsg);
        if (updated != null)
            updateShard(updated);

        HandOff.writeToFile("Node " + this.nodeID + " ClusterState: waiting for acknowledgement from cluster", this.logPath);
        this.clusterRaftOut.take();
        HandOff.writeToFile("Node " + this.nodeID + " ClusterState: waiting for acknowledgement from Shard", this.logPath);
        this.shardRaftOut.take();

        HandOff.writeToFile("Node " + this.nodeID + " ClusterState: new config finalised", this.logPath);
        if (nodeMsg.client != null) {
            Response response = new Response("Cluster successfully updated");
            HandOff.sendToNode(nodeMsg.client, gson.toJson(response), this.logPath);
        }
    }

    public void replyConfig(Node client) {
        Config configMsg = new Config(cluster, version);

        try {
            HandOff.sendToNode(client, gson.toJson(configMsg), this.logPath);
        } catch (Exception e) {
            HandOff.writeToFile("Error sending config reply: " + e, this.logPath);
        }
    }

    /**
     * Reads one incoming message from the input pipe and applies the corresponding
     * cluster membership change if the message is of the supported {@link NodeMsg} type.
     *
     * <p>Supported actions are:</p>
     * <ul>
     *   <li>{@code Add} - add a node to the cluster and publish the updated membership</li>
     *   <li>{@code Remove} - remove a node from the cluster and publish the updated membership</li>
     * </ul>
     *
     * @throws Exception if a pipe write or cluster access operation fails
     */
    private Shard updateCluster(NodeMsg nodeMsg) throws Exception {
        String action = nodeMsg.action;

        // Tracks the shard impacted by the latest node operation.
        Shard updatedShard = null;

        // Add a node only when the supplied address information is valid.
        if (action.equals("Add") && nodeMsg.node.ip != null && nodeMsg.node.port != -1) {
            Node node = new Node(nodeMsg.node.id, nodeMsg.node.ip, nodeMsg.node.port);
            cluster.addNode(node);
            updatedShard = cluster.getShard(nodeMsg.node.id);
            version++;
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: added node " + nodeMsg.node.id, this.logPath);
        } else if (action.equals("Remove")) {
            // Remove a node from the cluster and publish the new stable membership view.
            updatedShard = cluster.getShard(nodeMsg.node.id);
            cluster.removeNode(nodeMsg.node.id);
            version++;
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: removed node " + nodeMsg.node.id, this.logPath);
        } else {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: unknown NodeMsg action " + nodeMsg.action, this.logPath);
            return null;
        }

        // update cluster
        HandOff.writeToFile("Node " + this.nodeID + " sending update message to raft cluster", this.logPath);
        clusterRaftIn.put(new message.Update("Update", cluster.getAllNodes(), version));

        // Dispatch shard-specific notifications after the cluster update completes.
        return updatedShard;
    }

    /**
     * Propagates shard-level state changes to the shard Raft pipeline when the local
     * node's assigned shard changes or when the shard membership has changed significantly.
     *
     * @param updatedShard the shard affected by the most recent node add/remove action
     * @throws Exception if writing to the shard Raft pipe fails
     */
    private void updateShard(Shard updatedShard) throws Exception {
        if (updatedShard == null) {
            return;
        }

        Shard shard = cluster.getShard(nodeID);

        // If the local node migrated to a different shard, or the current shard became
        // unexpectedly too small relative to its tracked size, publish the full distributed
        // data snapshot and the updated node list for the shard.
        if (!shard.id.equals(currShardID) || currShardSize > shard.size() + 1) {
            HandOff.writeToFile("Node " + this.nodeID + " sending Distribute message to Shard", this.logPath);
            /*
            this.pendingShardAckPipe = new Pipe();
            ShardRaft.put(new message.Update("Distribute", shard.getAllNodes(), this.pendingShardAckPipe, version));
            Message shardAck = this.pendingShardAckPipe.take();
            if (shardAck != null) {
                this.pendingShardAckPipe = null;
            }
            currShardID = shard.id;
            currShardSize = shard.size();
            */
        } else if (currShardID.equals(updatedShard.id)) {
            // When the local node remains in the same shard, only the affected shard's
            // node list needs to be propagated.
            HandOff.writeToFile("Node " + this.nodeID + " sending Update message to Shard", this.logPath);
            shardRaftIn.put(new message.Update("Update", updatedShard.getAllNodes(), version));
        }
    }

    /**
     * Main execution loop for the cluster state service.
     *
     * <p>The thread continuously waits for and processes incoming cluster membership
     * updates, swallowing individual exceptions so the service can keep running.</p>
     */
    @Override
    public void run() {
        while (true) {
            try {
                takeAndHandle();
            } catch (Exception e) {
                HandOff.writeToFile("ClusterState: error updating cluster: " + e.getMessage(), this.logPath);
            }
        }
    }
}
