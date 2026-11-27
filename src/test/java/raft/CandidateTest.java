package raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import communication.Pipe;
import message.Message;
import message.RequestVote;
import message.RequestVoteReply;
import testutil.Capture;
import testutil.KvFake;
import testutil.Nodes;

class CandidateTest {

    private final Capture capture;
    private RaftState state;
    private Candidate candidate;

    CandidateTest() throws Exception {
        capture = new Capture();
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
    }

    private void setUp(int peers) {
        var nodes = Nodes.map(Nodes.unreachable("A"));
        for (int i = 0; i < peers; i++)
            nodes.put("P" + i, capture.node("P" + i));
        state = new RaftState("A", new KvFake(), "Shard", nodes, new Pipe(), false);
        candidate = new Candidate(state);
    }

    private RequestVoteReply vote(String from, int term, boolean granted) {
        return new RequestVoteReply("Shard", term, from, granted, 0);
    }

    @Test
    void startElectionVotesForSelfAndRequestsVotesFromPeers() throws Exception {
        setUp(2);
        candidate.startElection();

        assertEquals(1, state.term);
        assertEquals("candidate", state.type);
        assertEquals("A", state.votedFor);

        List<Message> sent = capture.drain(300);
        assertEquals(2, sent.size());
        for (Message m : sent) {
            RequestVote rv = (RequestVote) m;
            assertEquals(1, rv.term);
            assertEquals("A", rv.senderId);
            assertEquals(-1, rv.lastLogIndex);
        }
    }

    @Test
    void majorityOfVotesMakesLeader() {
        setUp(2);
        candidate.startElection();
        assertTrue(candidate.requestVote(vote("P0", 1, true)));
        assertEquals("leader", state.type);
        assertNotNull(state.matchIndex);
    }

    @Test
    void duplicateAndRejectedVotesDoNotCount() {
        setUp(4); // 5 voters, need 3 votes
        candidate.startElection();
        candidate.requestVote(vote("P0", 1, true));
        assertFalse(candidate.requestVote(vote("P0", 1, true)));
        candidate.requestVote(vote("P1", 1, false));
        assertEquals("candidate", state.type);

        candidate.requestVote(vote("P2", 1, true));
        assertEquals("leader", state.type);
    }

    @Test
    void staleRepliesAreIgnored() {
        setUp(2);
        candidate.startElection();
        candidate.startElection(); // term 2
        assertFalse(candidate.requestVote(vote("P0", 1, true)));
        assertEquals("candidate", state.type);
    }

    @Test
    void higherTermRevertsToFollower() {
        setUp(2);
        candidate.startElection();
        assertFalse(candidate.requestVote(vote("P0", 5, false)));
        assertEquals("follower", state.type);
        assertEquals(5, state.term);
        assertNull(state.votedFor);
    }
}
