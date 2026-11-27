package message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import cluster.Node;

/**
 * Point-in-time image of a Raft node's applied state.
 *
 * A snapshot replaces every committed log entry up to and including
 * {@link #lastIncludedIndex}. It is written to disk so a restarting node can
 * restore from it, sent to lagging followers inside an {@link InstallSnapshot}
 * RPC, and pushed through the Raft output pipe so the local state machine
 * (StateMachine for the shard level, ClusterState for the cluster level) can
 * replace its in-memory state.
 */
public class Snapshot extends Message {

    /** Index of the last log entry covered by this snapshot. */
    public int lastIncludedIndex;

    /** Term of the last log entry covered by this snapshot. */
    public int lastIncludedTerm;

    /** Raft term the node was in when the snapshot was taken. */
    public int term;

    /** Candidate the node had voted for in {@link #term}, or null. */
    public String votedFor;

    /**
     * Ids of the Raft configuration voters at the time of the snapshot. The
     * voters are a subset of the active nodes, so addresses are stored once in
     * the message's node list.
     */
    private Set<String> voterIds;

    /** Ids of every node active in the Raft configuration at the time of the snapshot. */
    private Set<String> activeNodeIds;

    /**
     * Snapshot files written before node ids were introduced stored the full
     * maps under these names. Only read, so those files still restore; never
     * written (null fields are left out of the JSON).
     */
    @Deprecated
    private Map<String, Node> voters;

    /** See {@link #voters}. */
    @Deprecated
    private Map<String, Node> activeNodes;

    /** Key-value store contents produced by the committed DictMsg entries. */
    public Map<String, String> kvStore;

    /**
     * Ordered list of committed cluster membership changes. Replaying these on
     * top of the bootstrap seed nodes rebuilds the cluster's consistent-hash
     * ring exactly, since shard placement depends on the order of changes.
     */
    public List<MembershipOp> membershipOps;

    /**
     * Single committed cluster membership change.
     */
    public static class MembershipOp {
        /** Membership action, either Add or Remove. */
        public String action;

        /** Node that was added or removed. */
        public Node node;

        /**
         * Creates a membership change record.
         *
         * @param action Add or Remove
         * @param node node affected by the change
         */
        public MembershipOp(String action, Node node) {
            this.action = action;
            this.node = node;
        }
    }

    /**
     * Creates a snapshot holding the Raft metadata. The state machine fills
     * {@link #kvStore} or {@link #membershipOps} afterwards.
     *
     * @param lastIncludedIndex index of the last log entry covered
     * @param lastIncludedTerm term of the last log entry covered
     * @param term current Raft term of the node
     * @param votedFor candidate voted for in the current term
     * @param voters current Raft voter set
     * @param activeNodes every node in the current Raft configuration
     * @param version Raft configuration version at the time of the snapshot
     */
    public Snapshot(int lastIncludedIndex, int lastIncludedTerm, int term, String votedFor,
                    Map<String, Node> voters, Map<String, Node> activeNodes, int version) {
        super("Snapshot", version);
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.term = term;
        this.votedFor = votedFor;
        this.voterIds = ref(voters == null ? new HashMap<>() : voters);
        this.activeNodeIds = ref(activeNodes == null ? new HashMap<>() : activeNodes);
        this.kvStore = new HashMap<>();
        this.membershipOps = new ArrayList<>();
    }

    /**
     * Raft configuration voters at the time of the snapshot.
     *
     * @return a new map of the voters keyed by id (empty if none)
     */
    @SuppressWarnings("deprecation")
    public Map<String, Node> voters() {
        if (voterIds == null)
            return voters == null ? new HashMap<>() : new HashMap<>(voters);
        return deref(voterIds);
    }

    /**
     * Every node active in the Raft configuration at the time of the snapshot.
     *
     * @return a new map of the nodes keyed by id (empty if none)
     */
    @SuppressWarnings("deprecation")
    public Map<String, Node> activeNodes() {
        if (activeNodeIds == null)
            return activeNodes == null ? new HashMap<>() : new HashMap<>(activeNodes);
        return deref(activeNodeIds);
    }
}
