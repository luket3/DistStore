package raft;


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
    public boolean requestVote(String[] messageParts) {
        // Parse RequestVote RPC parameters from messageParts
        // Format: RequestVote <term> <candidateId> <lastLogIndex> <lastLogTerm>
        if (messageParts.length < 5) {
            // Invalid message format
            return false;
        }

        int candidateTerm = Integer.parseInt(messageParts[1]);
        String candidateId = messageParts[2];
        int lastLogIndex = Integer.parseInt(messageParts[3]);
        int lastLogTerm = Integer.parseInt(messageParts[4]);

        // Check current term and update if necessary
        if (candidateTerm > raftState.term) {
            raftState.term = candidateTerm;
            raftState.type = "follower";
            raftState.votedFor = null;
        }

        // Reject if candidate's term is less than current term
        if (candidateTerm < raftState.term) {
            return false;
        }
        
        // Check if candidate's log is at least as up-to-date as receiver's log
        int receiverLastLogIndex = raftState.log.getLastIdx();
        int receiverLastLogTerm = raftState.log.getLastTerm();

        boolean logUpToDate = (lastLogTerm > receiverLastLogTerm)
            || ((lastLogTerm == receiverLastLogTerm) 
            && lastLogIndex >= receiverLastLogIndex);

        // Vote for candidate if we haven't voted or already voted for this
        // candidate, and the candidate's log is up-to-date
        boolean voteGranted = logUpToDate && (
            raftState.votedFor == null || raftState.votedFor.equals(candidateId)
        );
        if (voteGranted) {
            raftState.votedFor = candidateId;
        }

        // Send vote grant status back to the candidate
        sendToNode(
            raftState.nodes.get(candidateId),
            "RequestVoteReply " + raftState.term + " " + raftState.id + " " + voteGranted
        );

        return voteGranted;
    }

    /**
     * Process an AppendEntries RPC from leader.
     *
     * @param messageParts the parsed RPC message parts
     * @return true if the RPC was successful, false otherwise
     */
    public boolean appendEntries(String[] messageParts) {
        // Parse AppendEntries RPC parameters from messageParts
        // Format: AppendEntries <term> <leaderId> <prevLogIndex>
        // <prevLogTerm> <leaderCommit> [entries...]
        if (messageParts.length < 6) {
            // Invalid message format
            return false;
        }

        int leaderTerm = Integer.parseInt(messageParts[1]);
        String leaderId = messageParts[2];
        int prevLogIndex = Integer.parseInt(messageParts[3]);
        int prevLogTerm = Integer.parseInt(messageParts[4]);
        int leaderCommit = Integer.parseInt(messageParts[5]);

        // Update term and revert to follower if we see a higher term
        if (leaderTerm > raftState.term) {
            raftState.term = leaderTerm;
            raftState.type = "follower";
            raftState.votedFor = null;
        } else if (leaderTerm < raftState.term) {
            // Reject if leader's term is less than current term
            return false;
        }

        if (raftState.leader == null || !raftState.leader.id.equals(leaderId)) {
            // Update leader information if this is a new leader
            raftState.leader = raftState.nodes.get(leaderId);
            raftState.log.clearUncommitted();
        }

        // Check prevLogIndex and prevLogTerm match
        boolean logMatch = true;
        if (prevLogIndex >= 0) {
            if (prevLogIndex >= raftState.log.getSize()) {
                logMatch = false;
            } else if (raftState.log.get(prevLogIndex).term != prevLogTerm) {
                logMatch = false;
            }
        }

        if (logMatch) {
            // commands are in format:
            // [ClientCommand <insert command>,ClientCommand <insert command>,...]
            if (messageParts.length >= 7 && !messageParts[6].equals("")) {
                String commands = messageParts[6];
                String[] split = commands.substring(1,commands.length() - 1)
                                         .split(",");

                raftState.log.clearTo(prevLogIndex);
                for (String cmd : split)
                    raftState.log.appendEntry(cmd, leaderTerm);
            }

            // commit upto leadercommit
            raftState.log.commitEntries(leaderCommit);
        }

        sendToNode(
            raftState.leader,
            "AppendEntriesReply " + raftState.term  + " "
                                  + raftState.id + " " 
                                  + logMatch + " " 
                                  + raftState.log.getLastIdx()
        );
        return true;
    }

    public void handToLeader(String message) {
        if (raftState.leader != null) {
            sendToNode(raftState.leader, message);
        } else {
            // No known leader, could buffer the message or ignore
        }
    }
}
