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

    /** Constants for heartbeat and election timeout intervals */
    private static final int HEARTBEAT_INTERVAL_MS = 1000; // 1 second heartbeat interval
    private static final int ELECTION_TIMEOUT_MIN_MS = 2000; // 2 seconds
    private static final int ELECTION_TIMEOUT_MAX_MS = 5000; // 5 seconds

    /**
     * Creates a Raft runtime instance for one node.
     *
     * @param inPipe input pipe that receives Raft protocol traffic
     * @param outPipe output pipe used by the node for outbound Raft messages
     * @param nodeId identifier of the local node
     * @param level Raft pipeline classification, such as Shard or Cluster
     * @param configData known peer configuration for this runtime instance
     * @param ackPipe pipe used to report acknowledgement state back to callers
     * @param learner whether this node participates as a learner-only peer
     */
    public Raft(
        Pipe inPipe,
        Pipe outPipe,
        String nodeId,
        String level,
        Map<String,Node> configData,
        Pipe ackPipe,
        boolean learner
    ) {
        node = new RaftNode(nodeId, outPipe, level, configData, ackPipe, learner);
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
                    // Randomize election timeout between 2 and 5 seconds for
                    // followers and candidates.
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