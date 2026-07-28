package raft;

/*
 * File: Raft.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Partial Raft role implementation for handling incoming RPCs.
 */

import communication.Pipe;
import message.Message;
import java.util.Map;
import cluster.Node;

/**
 * Partial Raft role implementation for handling incoming RPCs.
 *
 * <p>This class models a node's Raft role
 * (follower/candidate/leader) and provides an entry point for processing
 * messages received via a pipe.</p>
 */
public class Raft implements Runnable {
    RaftNode node;
    Pipe inPipe;
    Pipe outPipe;
    private long lastHeartbeatTime = -1;
    private static final int HEARTBEAT_INTERVAL_MS = 1000; // 1 second heartbeat interval
    private static final int ELECTION_TIMEOUT_MIN_MS = 2000; // 2 seconds
    private static final int ELECTION_TIMEOUT_MAX_MS = 5000; // 5 seconds
    String nodeId;
    String level;

    public Raft(
        Pipe inPipe,
        Pipe outPipe,
        String nodeId,
        String level,
        Map<String,Node> configData,
        Pipe ackPipe
    ) {
        node = new RaftNode(nodeId, outPipe, level, configData, ackPipe);
        this.inPipe = inPipe;
        this.outPipe = outPipe;
        this.nodeId = nodeId;
        this.level = level;
    }

    /**
     * Start listening for incoming messages via the pipe and handle Raft
     * timeouts. If no message is received within the election timeout the
     * node becomes a candidate and an election is started.
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