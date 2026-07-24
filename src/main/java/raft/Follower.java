package raft;

import message.Message;
import message.RequestVote;
import message.AppendEntries;
import message.RequestVoteReply;
import message.AppendEntriesReply;
import message.DictMsg;

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
        System.out.println(raftState.level + ": Follower " + raftState.id + " received: " + raftState.gson.toJson(RVmsg));

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
            raftState.nodes.get(RVmsg.candidateId),
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
        System.out.println(raftState.level + ": Follower " + raftState.id + " received: " + raftState.gson.toJson(AEmsg));

        // Update term and revert to follower if we see a higher term
        if (AEmsg.term > raftState.term) {
            raftState.term = AEmsg.term;
            raftState.type = "follower";
            raftState.votedFor = null;
        } else if (AEmsg.term < raftState.term) {
            // Reject if leader's term is less than current term
            return false;
        }

        if (raftState.leader == null || !raftState.leader.id.equals(AEmsg.leaderId)) {
            // Update leader information if this is a new leader
            raftState.leader = raftState.nodes.get(AEmsg.leaderId);
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

                raftState.log.clearTo(AEmsg.prevLogIndex);
                for (LogEntry entry : AEmsg.entries) {
                    raftState.log.appendEntry(entry.msg, entry.term);
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
        DictMsg cmdMsg = (DictMsg) message;
        System.out.println(raftState.level + ": Follower " + raftState.id + " received: " + raftState.gson.toJson(cmdMsg));

        if (raftState.leader != null) {
            sendToNode(raftState.leader, raftState.gson.toJson(cmdMsg));
        } else {
            // No known leader, could buffer the message or ignore
        }
    }
}
