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
 * Leader role for Raft consensus.
 */
public class Leader extends Role {

    public Leader(RaftState raftState) {
        super(raftState);
    }

    public void appendLogEntry(Message msg) {
        // Handle a client command when this node is the leader.
        // Format: ClientCommand <command>
        HandOff.writeToFile(
            raftState.level + ": Leader " + raftState.id + " appending log entry",
            raftState.getLogFilePath()
        );

        // Append the command to the log as an uncommitted entry
        raftState.log.appendEntry(msg, raftState.term);
        // Update own matchIndex to reflect the new entry
        raftState.matchIndex.put(raftState.id, raftState.log.getLastIdx());

        broadcastAppendEntries();
    }

    public void appendEntries(Message messageParts) {
        // Process a follower's AppendEntries response when this node is
        // acting as leader.
        // Format: AppendEntries <term> <senderID> <success> <matchIndex>
        AppendEntriesReply AEReply = (AppendEntriesReply) messageParts;

        // Update term and revert to follower if we see a higher term
        if (AEReply.term > raftState.term) {
            raftState.term = AEReply.term;
            raftState.type = "follower";
            raftState.votedFor = null;
            return;
        }

        // If AppendEntries was successful, update match index for that follower
        if (AEReply.success) {

            // register follower as having entries up to senderMatchIdx
            raftState.matchIndex.put(AEReply.senderId, Math.max(raftState.matchIndex.get(AEReply.senderId), AEReply.matchIndex));
            raftState.nextIndex.put(AEReply.senderId, raftState.matchIndex.get(AEReply.senderId) + 1);

            // check if a learner has caught up and can be promoted
            raftState.appendLearnerPromotion();

            // commit such that majority nodes have log entry
            for (int i = raftState.log.getLastIdx(); i > raftState.log.getCommitIdx(); i--) {

                long count = 0;
                boolean majority = false;

                if (!raftState.jointConfig) {
                    // Count only voters, not learners
                    int threshold = (raftState.voters.size() / 2) + 1;
                    for (String nid : raftState.voters.keySet()) {
                        int v = raftState.matchIndex.getOrDefault(nid, -1);
                        if (v >= i) count++;
                    }
                    if (count >= threshold) majority = true;
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

                if (majority && raftState.log.get(i).term == raftState.term) {
                    // Commit the entries up to i
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
