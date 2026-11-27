package raft;

import communication.Pipe;
import message.Message;
import java.util.Map;
import cluster.Node;

/**
 * Runnable wrapper that drives one Raft protocol instance for a node.
 *
 * The class repeatedly waits on its input pipe for incoming Raft messages,
 * sends periodic leader heartbeats when the node is the leader, and triggers
 * an election timeout transition when the node is a follower or candidate.
 */
public class Raft implements Runnable {
    /** The Raft node instance */
    private RaftNode node;

    /** The input pipe for receiving Raft messages */
    private Pipe inPipe;

    /** Timestamp of the last heartbeat sent by the leader */
    private long lastHeartbeatTime = -1;

    /**
     * Interval between leader heartbeats. Overridable with
     * {@code -Draft.heartbeatMs=N}.
     */
    public static final int HEARTBEAT_INTERVAL_MS = Integer.getInteger("raft.heartbeatMs", 100);

    /**
     * Bounds of the randomized election timeout. The minimum is several
     * heartbeats long so a delayed heartbeat or two does not trigger an
     * election, and the range is wide so candidates rarely time out together.
     * Overridable with {@code -Draft.electionMinMs=N} and
     * {@code -Draft.electionMaxMs=N}.
     */
    public static final int ELECTION_TIMEOUT_MIN_MS = Integer.getInteger("raft.electionMinMs", 400);
    public static final int ELECTION_TIMEOUT_MAX_MS = Math.max(
        ELECTION_TIMEOUT_MIN_MS + 1, Integer.getInteger("raft.electionMaxMs", 800));

    /**
     * Creates a Raft runtime instance for one node.
     *
     * @param inPipe input pipe that receives Raft protocol traffic
     * @param stateMachine state machine committed entries are applied to
     * @param nodeId identifier of the local node
     * @param level Raft pipeline classification, such as Shard or Cluster
     * @param configData known peer configuration for this runtime instance
     * @param ackPipe pipe used to report acknowledgement state back to callers
     * @param learner whether this node participates as a learner-only peer
     */
    public Raft(
        Pipe inPipe,
        ReplicatedStateMachine stateMachine,
        String nodeId,
        String level,
        Map<String,Node> configData,
        Pipe ackPipe,
        boolean learner
    ) {
        node = new RaftNode(nodeId, stateMachine, level, configData, ackPipe, learner);
        this.inPipe = inPipe;
    }

    /**
     * Runs the Raft protocol loop for the current node.
     *
     * The loop applies a leader heartbeat schedule when the node is leader,
     * otherwise it waits for a randomized election timeout. If a message
     * arrives before the timeout expires, it is dispatched to the underlying
     * RaftNode state machine. If the timeout expires first, a follower or
     * candidate escalates into an election.
     */
    @Override
    public void run() {
        while (true) {
            try {
                long timeoutMs;
                long now = System.currentTimeMillis();

                if (node.getRole().equals("leader")) {
                    // Leaders send heartbeats on a fixed interval.
                    if (lastHeartbeatTime == -1
                            || now - lastHeartbeatTime >= HEARTBEAT_INTERVAL_MS) {
                        lastHeartbeatTime = now;
                        node.sendHeartbeat();
                    }
                    timeoutMs = HEARTBEAT_INTERVAL_MS;
                } else {
                    // Randomize the election timeout for followers and candidates.
                    timeoutMs = ELECTION_TIMEOUT_MIN_MS
                            + (int) (Math.random()
                                    * (ELECTION_TIMEOUT_MAX_MS
                                            - ELECTION_TIMEOUT_MIN_MS));
                }

                Message message = inPipe.take(Math.max(1, timeoutMs));

                // If we get a message (not null), process it
                if (message != null) {
                    // Process the message through the Raft node
                    node.handleMessage(message);
                } else {
                    // Timeout occurred: no message received within the
                    // election timeout

                    if ((node.getRole().equals("follower")
                        || node.getRole().equals("candidate"))) {
                        // Follower or candidate timeout: start election
                        node.startElection();
                    }
                    // Leaders do not timeout the same way because they
                    // send heartbeats
                }

            } catch (InterruptedException e) {
                // Thread was interrupted, exit gracefully
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Handle other exceptions
                e.printStackTrace();
                // Continue listening
            }
        }
    }
}