package cluster;

import communication.HandOff;
import communication.Pipe;
import message.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import message.NodeMsg;
import message.Config;
import message.Response;
import message.Snapshot;
import message.SplitShard;
import message.Update;
import raft.ReplicatedStateMachine;
import com.google.gson.Gson;

/**
 * Maintains the local node's view of the cluster membership state.
 *
 * The class has two halves that run on different threads:
 *
 * <ul>
 *   <li>The state machine half ({@link #apply}, {@link #fillSnapshot},
 *       {@link #restore}) is called synchronously by the cluster Raft thread.
 *       It applies each committed Add or Remove to the consistent-hash ring
 *       and works out which follow-up messages the change requires, so the
 *       ring always matches the committed log and can be snapshotted
 *       directly.</li>
 *   <li>The coordinator half ({@link #run}) runs on its own thread. It carries
 *       out those follow-up messages, which means waiting for the cluster and
 *       shard Raft groups to acknowledge the new configuration. That waiting
 *       cannot happen on the Raft thread, because the Raft thread is the one
 *       that produces the acknowledgement. It also answers Config requests
 *       from clients.</li>
 * </ul>
 *
 * Ring state is shared between the two threads, so every access to it is
 * synchronized on this object.
 */
public class ClusterState implements Runnable, ReplicatedStateMachine {
    /** input pipe for Config requests and follow-up work queued by {@link #apply} */
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

    /** bootstrap nodes in the order they were first added, used to rebuild the ring from a snapshot */
    private List<Node> seedNodes;

    /** applied membership changes in commit order; replayed on top of the seeds to rebuild the ring */
    private List<Snapshot.MembershipOp> membershipOps;

    /**
     * Follow-up work produced by applying one membership change, executed by
     * the coordinator thread. Everything it needs is captured at apply time so
     * later changes to the ring cannot affect it.
     */
    private static class PendingUpdate extends Message {
        /** full cluster membership after the change, sent to the cluster Raft */
        final Map<String, Node> clusterNodes;

        /** Update or SplitShard message for the shard Raft, or null if the shard is unaffected */
        final Message shardUpdate;

        /** client to notify once the change is finalised, or null */
        final Node client;

        PendingUpdate(Map<String, Node> clusterNodes, Message shardUpdate, Node client, int version) {
            super("PendingUpdate", version);
            this.clusterNodes = clusterNodes;
            this.shardUpdate = shardUpdate;
            this.client = client;
        }
    }

    /**
     * Creates a new cluster state manager for the local node.
     *
     * @param inPipe input pipe used to receive Config requests and queued follow-up work
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
        this.nodeID = nodeID;
        this.shardRaftIn = shardRaftIn;
        this.clusterRaftIn = clusterRaftIn;
        this.shardRaftOut = shardRaftOut;
        this.clusterRaftOut = clusterRaftOut;
        this.gson = new Gson();
        this.logPath = "logs/ClusterState.log";
        this.seedNodes = new ArrayList<>(nodes.values());
        rebuild(new ArrayList<>());
    }

    /**
     * Rebuilds the ring from the seed nodes plus the supplied membership
     * changes, replayed in order so shard placement matches the original
     * sequence of updates.
     *
     * @param ops membership changes to replay
     * @throws Exception if the ring cannot be rebuilt
     */
    private void rebuild(List<Snapshot.MembershipOp> ops) throws Exception {
        ConsistentHashMap rebuilt = new ConsistentHashMap();
        for (Node n : seedNodes) {
            rebuilt.addNode(n);
        }
        for (Snapshot.MembershipOp op : ops) {
            if ("Add".equals(op.action))
                rebuilt.addNode(new Node(op.node.id, op.node.ip, op.node.port));
            else if ("Remove".equals(op.action))
                rebuilt.removeNode(op.node.id);
        }

        this.cluster = rebuilt;
        this.membershipOps = new ArrayList<>(ops);
        this.version = ops.size();
        refreshCurrShard();
    }

    /**
     * Records the shard the local node currently belongs to and its size.
     */
    private void refreshCurrShard() {
        Shard currShard = cluster.getShardWithNode(nodeID);
        this.currShardID = currShard == null ? null : currShard.id;
        this.currShardSize = currShard == null ? -1 : currShard.getAllNodes().size();
    }

    /**
     * Applies a committed membership change to the ring. Called on the cluster
     * Raft thread. Any follow-up messages are queued for the coordinator
     * thread rather than sent here, since they wait on Raft acknowledgements.
     *
     * @param msg committed log payload; only NodeMsg is applied
     */
    @Override
    public synchronized void apply(Message msg) {
        if (!msg.type.equals("NodeMsg")) {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: unsupported message type " + msg.type, this.logPath);
            return;
        }

        try {
            NodeMsg nodeMsg = (NodeMsg) msg;
            Map<String, Shard> result = updateCluster(nodeMsg);
            if (result == null)
                return; // rejected change: nothing changed, so there is nothing to wait for

            Shard shard = cluster.getShardWithNode(nodeID);
            if (shard != null) {
                Message shardUpdate = shardUpdateFor(result, shard, nodeMsg.action.equals("Remove"));
                inPipe.put(new PendingUpdate(cluster.getAllNodes(), shardUpdate, nodeMsg.client, version));
            }
            refreshCurrShard();
        } catch (Exception e) {
            HandOff.writeToFile("ClusterState: " + nodeID + " error applying membership change: " + e.getMessage(), this.logPath);
        }
    }

    /**
     * Copies the applied membership changes into a snapshot.
     *
     * @param snapshot snapshot being built by the cluster Raft log
     */
    @Override
    public synchronized void fillSnapshot(Snapshot snapshot) {
        snapshot.membershipOps = new ArrayList<>(membershipOps);
    }

    /**
     * Replaces the ring with the one described by a snapshot.
     *
     * @param snapshot snapshot restored from disk or installed from the leader
     */
    @Override
    public synchronized void restore(Snapshot snapshot) {
        try {
            rebuild(snapshot.membershipOps == null ? new ArrayList<>() : snapshot.membershipOps);
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: restored from snapshot, version " + this.version, this.logPath);
        } catch (Exception e) {
            HandOff.writeToFile("ClusterState: " + nodeID + " failed to restore snapshot: " + e.getMessage(), this.logPath);
        }
    }

    /**
     * Processes a single message from the input pipe on the coordinator
     * thread.
     *
     * A Config message is answered with the current cluster view. A
     * PendingUpdate publishes the new membership to the cluster Raft, waits for
     * its acknowledgement, does the same for the shard Raft when the local
     * shard changed, and finally notifies the client if one is attached.
     *
     * @throws Exception if an input or output pipe operation fails while handling
     *     the message
     */
    public void takeAndHandle() throws Exception {
        // Block until a new message arrives on the input pipe.
        Message message = inPipe.take();
        if (message == null)
            return;

        if (message.type.equals("Config")) {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: replying with cluster config", this.logPath);
            Config configMsg = (Config) message;
            replyConfig(configMsg.client);
            return;
        }
        if (!(message instanceof PendingUpdate)) {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: unsupported message type " + message.type, this.logPath);
            return;
        }

        PendingUpdate update = (PendingUpdate) message;

        // send update message to cluster Raft pipe and wait for it to be applied
        HandOff.writeToFile("Node " + this.nodeID + " sending update message to raft cluster", this.logPath);
        clusterRaftIn.put(new Update(update.clusterNodes, update.version, false));
        HandOff.writeToFile("Node " + this.nodeID + " ClusterState: waiting for acknowledgement from cluster for version: " + update.version, this.logPath);
        this.clusterRaftOut.take();

        // update the shard if the change affected it and wait for it to be applied
        if (update.shardUpdate != null) {
            shardRaftIn.put(update.shardUpdate);
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: waiting for acknowledgement from Shard for version: " + update.version, this.logPath);
            this.shardRaftOut.take();
        }

        // send response to client if one is attached to the NodeMsg request.
        HandOff.writeToFile("Node " + this.nodeID + " ClusterState: new config finalised", this.logPath);
        if (update.client != null) {
            Response response = new Response("Cluster successfully updated");
            HandOff.sendToNode(update.client, gson.toJson(response), this.logPath);
        }
    }

    /**
     * Sends the current cluster snapshot and version to the requesting client.
     *
     * @param client destination node for the configuration response
     */
    public void replyConfig(Node client) {
        // serialize under the lock so the ring cannot change mid-serialization
        String json;
        synchronized (this) {
            json = gson.toJson(new Config(cluster, version));
        }

        try {
            HandOff.sendToNode(client, json, this.logPath);
        } catch (Exception e) {
            HandOff.writeToFile("Error sending config reply: " + e, this.logPath);
        }
    }

    /**
     * Applies a single membership change to the local consistent-hash state.
     *
     * Supported actions are Add and Remove. Add validates the node address,
     * inserts the node into the local cluster model and increments the
     * version. Remove removes the requested node from the local view and
     * increments the version. Each accepted change is recorded for snapshots.
     *
     * @param nodeMsg membership change request to apply
     * @return the result of the membership change, which may contain a new
     *     shard if a split occurred, or null if the change was rejected
     * @throws Exception if a cluster access operation fails
     */
    private Map<String, Shard> updateCluster(NodeMsg nodeMsg) throws Exception {
        String action = nodeMsg.action;

        // Add a node only when the supplied address information is valid.
        Map<String, Shard> result = null;
        if (action.equals("Add") && nodeMsg.node.ip != null && nodeMsg.node.port != -1) {
            Node node = new Node(nodeMsg.node.id, nodeMsg.node.ip, nodeMsg.node.port);
            result = cluster.addNode(node);
            membershipOps.add(new Snapshot.MembershipOp("Add", node));
            version++;
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: added node " + nodeMsg.node.id, this.logPath);
        } else if (action.equals("Remove")) {
            // Remove a node from the cluster and publish the new stable membership view.
            // Re-homing a removed shard's leftover nodes can split the shard they
            // join; report the split involving this node's shard, if any
            result = new HashMap<>();
            List<Shard[]> splits = new ArrayList<>();
            result.put("old", cluster.removeNode(nodeMsg.node.id, splits));
            Shard mine = cluster.getShardWithNode(nodeID);
            for (Shard[] split : splits) {
                if (mine != null && (split[0].id.equals(mine.id) || split[1].id.equals(mine.id))) {
                    result.put("splitIn", split[0]);
                    result.put("splitNew", split[1]);
                }
            }
            membershipOps.add(new Snapshot.MembershipOp("Remove", new Node(nodeMsg.node.id, null, -1)));
            version++;
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: removed node " + nodeMsg.node.id, this.logPath);
        } else {
            HandOff.writeToFile("Node " + this.nodeID + " ClusterState: unknown NodeMsg action " + nodeMsg.action, this.logPath);
            return null;
        }

        return result;
    }

    /**
     * Works out which message, if any, the shard Raft needs after a membership
     * change.
     *
     * <ul>
     *   <li>If the local node's old shard was removed, it gets a shard removal
     *       Update. If its new shard split while the removed shard's nodes were
     *       being re-homed, the Update lists both halves, because the node
     *       joins the group that is splitting and must be able to reach its
     *       leader until the split completes.</li>
     *   <li>If the local node's shard split, whether from an Add or from a
     *       removed shard's nodes being re-homed into it, a SplitShard message
     *       is produced.</li>
     *   <li>If the local node's shard otherwise changed size, an Update
     *       carrying the shard's node list is produced.</li>
     * </ul>
     *
     * @param result the result of the last membership change; holds "old"
     *     and "new" for an Add that split, "old" plus optionally "splitIn" and
     *     "splitNew" for a Remove
     * @param shard the shard the local node belongs to after the change
     * @param removal whether the change was a Remove
     * @return the shard Raft message, or null if the shard is unaffected
     */
    private Message shardUpdateFor(Map<String, Shard> result, Shard shard, boolean removal) {
        // the split, if any, that this change caused
        Shard splitIn = removal ? result.get("splitIn") : result.get("old");
        Shard splitNew = removal ? result.get("splitNew") : result.get("new");
        boolean inSplit = splitIn != null && splitNew != null
            && (splitIn.id.equals(shard.id) || splitNew.id.equals(shard.id));

        // the node's previous shard was removed and it has been re-homed
        Shard removedFrom = result.get("old");
        boolean rehomed = removal && currShardID != null && removedFrom != null
            && currShardID.equals(removedFrom.id) && !currShardID.equals(shard.id);

        if (rehomed) {
            HandOff.writeToFile("Node " + this.nodeID + " queueing shard removal Update message for Shard", this.logPath);
            Map<String, Node> joining = shard.getAllNodes();
            if (inSplit) {
                joining = new HashMap<>(splitIn.getAllNodes());
                joining.putAll(splitNew.getAllNodes());
            }
            return new Update(joining, version, cluster.ringOwners(), cluster.shardMembers());
        }
        // the node's shard split, prepare it to divide its data
        if (inSplit) {
            HandOff.writeToFile("Node " + this.nodeID + " queueing SplitShard message for Shard", this.logPath);
            return new SplitShard(splitIn.getAllNodes(), splitNew.getAllNodes(), version,
                cluster.ringOwners(), splitIn.id, splitNew.id);
        }
        // check if node has moved to a different shard for any other reason
        if (currShardID != null && !this.currShardID.equals(shard.id)) {
            HandOff.writeToFile("Node " + this.nodeID + " queueing shard removal Update message for Shard", this.logPath);
            return new Update(shard.getAllNodes(), version, cluster.ringOwners(), cluster.shardMembers());
        }
        // check if shard has shrunk or grown beyond the previous tracked size
        if (currShardID == null || shard.getAllNodes().size() != this.currShardSize) {
            HandOff.writeToFile("Node " + this.nodeID + " queueing Update message for Shard", this.logPath);
            return new Update(shard.getAllNodes(), version, false);
        }
        return null;
    }

    /**
     * Runs the coordinator loop for the cluster state component.
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
