package raft;

import communication.HandOff;
import message.Message;
import message.RequestVote;
import message.AppendEntries;
import message.RequestVoteReply;
import message.AppendEntriesReply;

/**
 * Follower role for Raft consensus.
 */
public class Follower extends Role {

    public Follower(RaftState raftState) {
        super(raftState);
    }

    /**
     * Process a RequestVote RPC from candidate.
     *
     * @param messageParts the parsed RPC message parts
     * @return true if vote was granted, false otherwise
     */
    public boolean requestVote(Message message) {
        // Parse RequestVote RPC parameters from messageParts
        // Format: RequestVote <term> <candidateId> <lastLogIndex> <lastLogTerm>
        RequestVote RVmsg = (RequestVote) message;
        HandOff.writeToFile(
            raftState.level + ": Follower " + raftState.id + " received: " + raftState.gson.toJson(RVmsg),
            raftState.getLogFilePath()
        );

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
            raftState.votedFor == null || raftState.votedFor.equals(RVmsg.candidateId)
        );
        if (voteGranted) {
            raftState.votedFor = RVmsg.candidateId;
        }

        // Send vote grant status back to the candidate
        RequestVoteReply RVReply = new RequestVoteReply(raftState.level, raftState.term, raftState.id, voteGranted, raftState.version);
        sendToNode(
            raftState.allNodes.get(RVmsg.candidateId),
            raftState.gson.toJson(RVReply)
        );

        return voteGranted;
    }

    /**
     * Process an AppendEntries RPC from leader.
     *
     * @param messageParts the parsed RPC message parts
     * @return true if the RPC was successful, false otherwise
     */
    public boolean appendEntries(Message message) {
        // Parse AppendEntries RPC parameters from messageParts
        // Format: AppendEntries <term> <leaderId> <prevLogIndex>
        // <prevLogTerm> <leaderCommit> [entries...]
        AppendEntries AEmsg = (AppendEntries) message;

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

        if (raftState.leader == null || !raftState.leader.id.equals(AEmsg.leaderId)) {
            // Update leader information if this is a new leader
            raftState.leader = raftState.allNodes.get(AEmsg.leaderId);
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

        if (logMatch) {
            // commands are in format:
            // [ClientCommand <insert command>,ClientCommand <insert command>,...]
            if (AEmsg.entries != null && AEmsg.entries.size() > 0) {
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

        AppendEntriesReply AEReply = new AppendEntriesReply(raftState.level, raftState.term, raftState.id, logMatch, raftState.log.getLastIdx(), raftState.version);
        sendToNode(
            raftState.leader,
            raftState.gson.toJson(AEReply)
        );
        return true;
    }

    public void handToLeader(Message message) {

        if (raftState.leader != null) {
            sendToNode(raftState.leader, raftState.gson.toJson(message));
        } else {
            // No known leader, could buffer the message or ignore
        }
    }
}
