package raft;

import message.Message;
import message.RequestVote;
import communication.HandOff;
import message.AppendEntries;
import message.RequestVoteReply;
import message.AppendEntriesReply;

/**
 * Raft role handler for a non-leader node that receives election requests,
 * applies replicated log entries from a leader, and forwards client traffic to
 * the current known leader.
 */
public class Follower extends Role {

    /**
     * Creates a follower role for the supplied Raft state.
     *
     * @param raftState shared Raft runtime state for the local node
     */
    public Follower(RaftState raftState) {
        super(raftState);
    }

    /**
     * Processes a RequestVote RPC received from a candidate.
     *
     * The follower upgrades its term when needed, validates the candidate's
     * log freshness, and returns a RequestVoteReply that reports whether the
     * vote was granted.
     *
     * @param RVmsg RequestVote RPC to evaluate
     * @return true when the vote is granted, false when the request is stale or
     *     rejected
     */
    public boolean requestVote(RequestVote RVmsg) {
        // Check current term and update if necessary
        if (RVmsg.term > raftState.term) {
            raftState.term = RVmsg.term;
            raftState.type = "follower";
            raftState.votedFor = null;
        }

        // Reject if candidate's term is less than current term
        if (RVmsg.term < raftState.term) {
            return false;
        }
        
        // Check if candidate's log is at least as up-to-date as receiver's log
        int receiverLastLogIndex = raftState.log.getLastIdx();
        int receiverLastLogTerm = raftState.log.getLastTerm();
        boolean logUpToDate = (RVmsg.lastLogTerm > receiverLastLogTerm)
            || ((RVmsg.lastLogTerm == receiverLastLogTerm) 
            && RVmsg.lastLogIndex >= receiverLastLogIndex);

        // Vote for candidate if we haven't voted or already voted for this
        // candidate, and the candidate's log is up-to-date
        boolean voteGranted = logUpToDate && (
            raftState.votedFor == null || raftState.votedFor.equals(RVmsg.senderId)
        );
        if (voteGranted) {
            raftState.votedFor = RVmsg.senderId;
        }

        // Send vote grant status back to the candidate
        RequestVoteReply RVReply = new RequestVoteReply(raftState.level, raftState.term, raftState.id, voteGranted, raftState.version);
        sendToNode(
            raftState.activeNodes.get(RVmsg.senderId),
            raftState.gson.toJson(RVReply)
        );

        return voteGranted;
    }

    /**
     * Processes an AppendEntries RPC received from the current leader.
     *
     * The follower accepts the leader's term when it is current, validates the
     * previous log index and term to ensure log continuity, applies any new
     * entries, advances the commit point, and returns an AppendEntriesReply to
     * the leader.
     *
     * @param AEmsg AppendEntries RPC to evaluate
     * @return true when the append request is accepted and processed
     */
    public boolean appendEntries(AppendEntries AEmsg) {

        // Update term and revert to follower if we see a higher term
        if (AEmsg.term > raftState.term) {
            raftState.term = AEmsg.term;
            if (raftState.type != "learner")
                raftState.type = "follower";
            raftState.votedFor = null;
        } else if (AEmsg.term < raftState.term) {
            // Reject if leader's term is less than current term
            return false;
        }


        // Update leader information if this is a new leader
        if (raftState.leader == null || !raftState.leader.id.equals(AEmsg.senderId)) {
            raftState.leader = raftState.activeNodes.get(AEmsg.senderId);
            raftState.log.clearUncommitted();
        }

        // Check prevLogIndex and prevLogTerm match
        boolean logMatch = true;
        if (AEmsg.prevLogIndex >= 0) {
            if (AEmsg.prevLogIndex >= raftState.log.getSize()) {
                logMatch = false;
            } else if (raftState.log.get(AEmsg.prevLogIndex).term != AEmsg.prevLogTerm) {
                logMatch = false;
            }
        }

        // If the log matches, append new entries and update commit index
        if (logMatch) {
            if (AEmsg.entries != null && AEmsg.entries.size() > 0) {

                // Append new entries to the log, replacing any conflicting entries
                int nextIndex = AEmsg.prevLogIndex + 1;
                for (int i = 0; i < AEmsg.entries.size(); i++) {
                    int targetIndex = nextIndex + i;
                    LogEntry incoming = AEmsg.entries.get(i);
                    LogEntry existing = raftState.log.get(targetIndex);
                    if (existing != null) {
                        if (existing.term != incoming.term) {
                            raftState.log.clearTo(targetIndex - 1);
                            for (int j = i; j < AEmsg.entries.size(); j++) {
                                raftState.log.appendEntry(
                                    AEmsg.entries.get(j).msg,
                                    AEmsg.entries.get(j).term
                                );
                            }
                            break;
                        }
                        continue;
                    }
                    raftState.log.appendEntry(incoming.msg, incoming.term);
                }
            }
            // commit upto leadercommit
            raftState.log.commitEntries(AEmsg.leaderCommit);
        }

        // Send AppendEntriesReply back to the leader
        AppendEntriesReply AEReply = new AppendEntriesReply(raftState.level, raftState.term, raftState.id, logMatch, raftState.log.getLastIdx(), raftState.version);
        sendToNode(
            raftState.leader,
            raftState.gson.toJson(AEReply)
        );
        return true;
    }

    /**
     * Forwards a message to the current known leader.
     *
     * This method is used when the follower needs to relay client or control
     * traffic to the established leader node.
     *
     * @param message outbound message to send to the leader
     */
    public void handToLeader(Message message) {

        if (raftState.leader != null) {
            sendToNode(raftState.leader, raftState.gson.toJson(message));
        } else {
            // No known leader Igornore message and log a warning.
            HandOff.writeToFile(
                "Level: " + raftState.level + " Node: " + raftState.id + "ignoring message, No leader Known", 
                raftState.getLogFilePath()
            );
        }
    }
}
