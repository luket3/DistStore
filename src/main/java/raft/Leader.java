package raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import cluster.Node;
import communication.HandOff;
import message.Message;
import message.AppendEntriesReply;
import message.AppendEntries;
import message.RaftConfig;

/**
 * Raft role handler responsible for leading the replicated log.
 *
 * The leader appends new client operations to its local log, publishes
 * AppendEntries RPCs to followers, tracks follower acknowledgement through
 * match and next indexes, and commits log positions once the configured
 * majority has confirmed them.
 */
public class Leader extends Role {

    public Leader(RaftState raftState) {
        super(raftState);
    }

    /**
     * Appends a client command to the leader's local log and initiates
     * replication to followers.
     *
     * @param msg client command to append to the replicated log
     */
    public void appendLogEntry(Message msg) {
        HandOff.writeToFile(
            raftState.level + ": Leader " + raftState.id + " appending log entry",
            raftState.getLogFilePath()
        );

        // Append the command to the log as an uncommitted entry
        raftState.log.appendEntry(msg, raftState.term);
        raftState.matchIndex.put(raftState.id, raftState.log.getLastIdx());
        broadcastAppendEntries();
    }

    /**
     * Processes an AppendEntriesReply from a follower and advances the leader's
     * replication state accordingly.
     *
     * The method upgrades the term on higher-term replies, updates the
     * follower's match and next indexes, checks whether the current log prefix
     * is now safe to commit, and retries an AppendEntries transmission when the
     * follower reports a log mismatch.
     *
     * @param AEReply AppendEntriesReply RPC to evaluate
     */
    public void appendEntries(AppendEntriesReply AEReply) {

        // Update term and revert to follower if we see a higher term
        if (AEReply.term > raftState.term) {
            raftState.term = AEReply.term;
            raftState.type = "follower";
            raftState.votedFor = null;
            return;
        }

        // Process Reply if it was successful, otherwise decrement nextIndex and retry AppendEntries
        if (AEReply.success) {

            // Update the follower's matchIndex and nextIndex based on the reply
            raftState.matchIndex.put(AEReply.senderId, Math.max(raftState.matchIndex.get(AEReply.senderId), AEReply.matchIndex));
            raftState.nextIndex.put(AEReply.senderId, raftState.matchIndex.get(AEReply.senderId) + 1);

            // check if a learner has caught up and can be promoted
            raftState.appendLearnerPromotion();

            // commit such that majority nodes have log entry
            for (int i = raftState.log.getLastIdx(); i > raftState.log.getCommitIdx(); i--) {
                long count = 0;
                boolean majority = false;
                if (!raftState.jointConfig) {
                    int threshold = (raftState.voters.size() / 2) + 1;
                    for (String nid : raftState.voters.keySet()) {
                        int v = raftState.matchIndex.getOrDefault(nid, -1);
                        if (v >= i) count++;
                    }
                    if (count >= threshold) majority = true;
                // joint configuration, need majority of both old and new nodes
                } else {
                    int oldCount = 0;
                    int nextCount = 0;
                    int oldThreshold = (raftState.oldNodes.size() / 2) + 1;
                    int nextThreshold = (raftState.nextNodes.size() / 2) + 1;
                    for (Map.Entry<String, Integer> e : raftState.matchIndex.entrySet()) {
                        int v = e.getValue();
                        if (v >= i) {
                            if (raftState.oldNodes.containsKey(e.getKey()))
                                oldCount++;
                            if (raftState.nextNodes.containsKey(e.getKey()))
                                nextCount++;
                        }
                    }
                    if (oldCount >= oldThreshold && nextCount >= nextThreshold)
                        majority = true;
                }

                // commit the log entry if a majority of nodes have it and it was appended in the current term
                if (majority && raftState.log.get(i).term == raftState.term) {
                    RaftConfig raftConfig = raftState.log.commitEntries(i);
                    if (raftConfig != null) {
                        if (raftConfig.jointConfig) {
                            raftState.appendNewConfig(raftConfig.nodes, false, false);
                        }
                    }
                    broadcastAppendEntries();
                    break;
                }
            }
        }
        // If the AppendEntriesReply was unsuccessful, decrement nextIndex and retry
        else {
            raftState.nextIndex.put(AEReply.senderId, raftState.nextIndex.get(AEReply.senderId) - 1);
            if (raftState.nextIndex.get(AEReply.senderId) < 0)
                raftState.nextIndex.put(AEReply.senderId, 0);

            // check if raft log needs to be truncated
            // Compute a truncate point based on majority of followers' nextIndex.
            // If a majority of nodes have nextIndex <= K, then their last
            // stored index is <= K-1; leader should truncate uncommitted
            // entries beyond K-1 to match the cluster.
            ArrayList<Integer> nextIndices = new ArrayList<>();
            for (Integer v : raftState.nextIndex.values())
                nextIndices.add(v);
            Collections.sort(nextIndices);

            int majorityPos = raftState.allNodes.size() / 2; // 0-based
            if (majorityPos < nextIndices.size()) {
                int K = nextIndices.get(majorityPos);
                int truncateTo = K - 1;

                if (truncateTo < raftState.log.getLastIdx()) {
                    HandOff.writeToFile(
                        raftState.level + ": Leader: truncating uncommitted entries to index " + truncateTo,
                        raftState.getLogFilePath()
                    );
                    raftState.log.clearTo(truncateTo);

                    // Ensure matchIndex and nextIndex are consistent with truncated log
                    int lastIdx = raftState.log.getLastIdx();
                    for (String nid : raftState.matchIndex.keySet()) {
                        int mi = raftState.matchIndex.get(nid);
                        if (mi > lastIdx)
                            raftState.matchIndex.put(nid, lastIdx);
                        int ni = raftState.nextIndex.get(nid);
                        if (ni > lastIdx + 1)
                            raftState.nextIndex.put(nid, lastIdx + 1);
                    }
                }
            }
            sendAppendEntries(raftState.allNodes.get(AEReply.senderId));
        }
    }

    /**
     * Sends AppendEntries RPCs to every peer node except the local leader.
     */
    public void broadcastAppendEntries() {
        HandOff.writeToFile(
            raftState.level + ": leader node: " + raftState.id + " broadcasting append entries message",
            raftState.getLogFilePath()
        );

        for (Node node : raftState.allNodes.values()) {
            if (!node.id.equals(raftState.id))
                sendAppendEntries(node);
        }
    }

    /**
     * Sends an AppendEntries RPC to one follower using the leader's current
     * log replication window.
     *
     * @param node follower receiving the AppendEntries RPC
     */
    public void sendAppendEntries(Node node) {
        // Send an empty AppendEntries RPC to all followers to maintain
        // leadership

        // update prevLogIndex and prevLogTerm
        int prevLogIndex = raftState.nextIndex.get(node.id) - 1;
        int prevLogTerm = (prevLogIndex >= 0) ?
                                raftState.log.get(prevLogIndex).term : 0;

        AppendEntries AEmsg = new AppendEntries(
            raftState.level,
            raftState.term,
            raftState.id,
            prevLogIndex,
            prevLogTerm,
            raftState.log.getCommitIdx(),
            raftState.log.get(raftState.nextIndex.get(node.id),
                                            raftState.log.getLastIdx())
        );

        sendToNode(node,
            raftState.gson.toJson(AEmsg)
        );
    }
}
