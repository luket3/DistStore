package cluster;
import communication.Pipe;
import message.Message;
import message.NodeMsg;

public class ClusterState implements Runnable {
    
    Pipe inPipe;
    ConsistentHashMap cluster;
    String currShardID;
    int currShardSize;
    int version;
    String NodeID;
    Pipe ShardRaft;
    Pipe clusterRaft;

    public ClusterState(Pipe inPipe, ConsistentHashMap cluster, String NodeID, Pipe ShardRaft, Pipe clusterRaft) throws Exception {
        this.inPipe = inPipe;
        this.cluster = cluster;
        this.NodeID = NodeID;
        this.ShardRaft = ShardRaft;
        this.version = 0;

        Shard currShard = cluster.getShard(NodeID);
        this.currShardID = currShard.id;
        this.currShardSize = currShard.size();
    }

    private void UpdateCluster() throws Exception {
        Message message = inPipe.take();
        if (message == null)
            return;

        if (!(message.type.equals("NodeMsg"))) {
            System.out.println("ClusterState: unsupported message type " + message.type);
            return;
        }

        NodeMsg nodeMsg = (NodeMsg) message;
        String action = nodeMsg.action;

        Shard updatedShard = null;
        if (action.equals("Add") && nodeMsg.node.ip != null && nodeMsg.node.port != -1) {
            Node node = new Node(nodeMsg.node.id, nodeMsg.node.ip, nodeMsg.node.port);
            cluster.addNode(node);
            updatedShard = cluster.getShard(nodeMsg.node.id);
            version++;
            clusterRaft.put(new message.UpdateNodes(cluster.getAllNodes(), "Update", version));
            System.out.println("ClusterState: added node " + nodeMsg.node.id);
        } else if (action.equals("Remove")) {
            updatedShard = cluster.getShard(nodeMsg.node.id);
            cluster.removeNode(nodeMsg.node.id);
            version++;
            clusterRaft.put(new message.UpdateNodes(cluster.getAllNodes(), "Update", version));
            System.out.println("ClusterState: removed node " + nodeMsg.node.id);
        } else {
            System.out.println("ClusterState: unknown NodeMsg action " + nodeMsg.action);
        }

        updateShard(updatedShard);
    }

    private void updateShard(Shard updatedShard) throws Exception {
        if (updatedShard == null) {
            return;
        }
        Shard shard = cluster.getShard(NodeID);

        if (!shard.id.equals(currShardID) || currShardSize > shard.size() + 1) {
            ShardRaft.put(new message.DistData(cluster, version));
            ShardRaft.put(new message.UpdateNodes(shard.getAllNodes(), "Update", version));
            currShardID = shard.id;
            currShardSize = shard.size();
        } else if (currShardID.equals(updatedShard.id)) {
            ShardRaft.put(new message.UpdateNodes(updatedShard.getAllNodes(), "Update", version));
        }
    }

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
