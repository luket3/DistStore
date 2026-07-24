package cluster;
import communication.Comm;
import communication.Pipe;
import message.Message;
import java.util.List;
import message.NodeMsg;
import message.Config;
import message.Reply;
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
    String NodeID;
    Pipe ShardRaft;
    Pipe clusterRaft;
    Gson gson;

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
    public ClusterState(Pipe inPipe, List<String> configData, String NodeID, Pipe ShardRaft, Pipe clusterRaft) throws Exception {
        this.inPipe = inPipe;
        this.cluster = new ConsistentHashMap();
        this.NodeID = NodeID;
        this.ShardRaft = ShardRaft;
        this.clusterRaft = clusterRaft;
        this.version = 0;
        this.gson = new Gson();

        // Initialize the cluster with the provided configuration data.
        for (String line : configData) {
            String[] split = line.split(",");
            cluster.addNode(new Node(
                    split[0],
                    split[1],
                    Integer.parseInt(split[2])
            ));
        }

        // Cache the node's initial shard identity and shard size for later comparisons.
        Shard currShard = cluster.getShard(NodeID);
        this.currShardID = currShard.id;
        this.currShardSize = currShard.size();

        ShardRaft.put(new message.UpdateShard("Init", currShard.getAllNodes(), version));
        clusterRaft.put(new message.UpdateShard("Init", cluster.getAllNodes(), version));
    }

    public void replyConfig(Comm comm) {
        Config configMsg = new Config(cluster, version);

        try {
            String json = gson.toJson(configMsg);
            comm.sendString(json);
        } catch (Exception e) {
            System.out.println("Error sending config reply: " + e);
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
    private void UpdateCluster() throws Exception {
        // Block until a new message arrives on the input pipe.
        Message message = inPipe.take();
        if (message == null)
            return;

        // Only NodeMsg messages are recognized for cluster topology updates.
        if (message.type.equals("Config")) {
            Reply replymsg = (Reply) message;
            replyConfig(replymsg.comm);
            return;
        }
        if (!(message.type.equals("NodeMsg"))) {
            System.out.println("Node " + this.NodeID + " ClusterState: unsupported message type " + message.type);
            return;
        }

        NodeMsg nodeMsg = (NodeMsg) message;
        String action = nodeMsg.action;

        // Tracks the shard impacted by the latest node operation.
        Shard updatedShard = null;

        // Add a node only when the supplied address information is valid.
        if (action.equals("Add") && nodeMsg.node.ip != null && nodeMsg.node.port != -1) {
            Node node = new Node(nodeMsg.node.id, nodeMsg.node.ip, nodeMsg.node.port);
            cluster.addNode(node);
            updatedShard = cluster.getShard(nodeMsg.node.id);
            version++;
            System.out.println("Node " + this.NodeID + " ClusterState: added node " + nodeMsg.node.id);
        } else if (action.equals("Remove")) {
            // Remove a node from the cluster and publish the new stable membership view.
            updatedShard = cluster.getShard(nodeMsg.node.id);
            cluster.removeNode(nodeMsg.node.id);
            version++;
            System.out.println("Node " + this.NodeID + " ClusterState: removed node " + nodeMsg.node.id);
        } else {
            System.out.println("Node " + this.NodeID + " ClusterState: unknown NodeMsg action " + nodeMsg.action);
            return;
        }
        System.out.println("Node " + this.NodeID + " sending update message to raft cluster");
        clusterRaft.put(new message.UpdateShard("Update", cluster.getAllNodes(), version));

        // Dispatch shard-specific notifications after the cluster update completes.
        updateShard(updatedShard);
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

        Shard shard = cluster.getShard(NodeID);

        // If the local node migrated to a different shard, or the current shard became
        // unexpectedly too small relative to its tracked size, publish the full distributed
        // data snapshot and the updated node list for the shard.
        if (!shard.id.equals(currShardID) || currShardSize > shard.size() + 1) {
            System.out.println("Node " + this.NodeID + " sending Distribute message to Shard");
            /*
            ShardRaft.put(new message.UpdateShard("Distribute", shard.getAllNodes(), cluster, version));
            currShardID = shard.id;
            currShardSize = shard.size();
            */
        } else if (currShardID.equals(updatedShard.id)) {
            // When the local node remains in the same shard, only the affected shard's
            // node list needs to be propagated.
            System.out.println("Node " + this.NodeID + " sending Update message to Shard");
            ShardRaft.put(new message.UpdateShard("Update", updatedShard.getAllNodes(), version));
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
                UpdateCluster();
            } catch (Exception e) {
                System.out.println("ClusterState: error updating cluster: " + e.getMessage());
            }
        }
    }
}
