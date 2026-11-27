package message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /** Raft configuration voters at the time of the snapshot. */
    public Map<String, Node> voters;

    /** Every node active in the Raft configuration at the time of the snapshot. */
    public Map<String, Node> activeNodes;

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
        this.voters = voters == null ? new HashMap<>() : new HashMap<>(voters);
        this.activeNodes = activeNodes == null ? new HashMap<>() : new HashMap<>(activeNodes);
        this.kvStore = new HashMap<>();
        this.membershipOps = new ArrayList<>();
    }
}
