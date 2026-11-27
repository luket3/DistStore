package cluster;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import communication.Pipe;
import message.Message;
import message.NodeMsg;
import message.Snapshot;
import message.SplitShard;
import message.Update;

class RemovalSplitTest {

    private static Map<String, Node> seeds() {
        Map<String, Node> s = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++)
            s.put("S" + i, new Node("S" + i, "localhost", 4000 + i));
        return s;
    }

    private static ConsistentHashMap build(List<Snapshot.MembershipOp> ops) throws Exception {
        ConsistentHashMap ring = new ConsistentHashMap();
        for (Node n : seeds().values())
            ring.addNode(n);
        for (Snapshot.MembershipOp op : ops) {
            if (op.action.equals("Add")) ring.addNode(op.node);
            else ring.removeNode(op.node.id);
        }
        return ring;
    }

    @Test
    void removalThatSplitsAShardSendsSplitShard() throws Exception {
        // find a membership history where removing a node re-homes a shard's
        // leftovers into a shard that then splits
        List<Snapshot.MembershipOp> ops = new ArrayList<>();
        String victim = null;
        List<Shard[]> splits = null;
        for (int i = 0; i < 60 && victim == null; i++) {
            ops.add(new Snapshot.MembershipOp("Add", new Node("N" + i, "localhost", 5000 + i)));
            for (Node n : build(ops).getAllNodes().values()) {
                List<Shard[]> found = new ArrayList<>();
                build(ops).removeNode(n.id, found);
                if (!found.isEmpty()) {
                    victim = n.id;
                    splits = found;
                    break;
                }
            }
        }
        assertNotNull(victim, "no membership history caused a split on removal");

        ConsistentHashMap before = build(ops);
        Shard removedShard = before.getShardWithNode(victim);
        ConsistentHashMap after = build(ops);
        after.removeNode(victim, new ArrayList<>());
        Shard[] split = splits.get(0);

        // a node that was already in the shard that split
        String stayer = before.getShardWithId(split[0].id).getAllNodes().keySet().iterator().next();
        // a node re-homed from the removed shard into one half of the split
        String rehomed = null;
        for (String id : removedShard.getAllNodes().keySet()) {
            Shard now = after.getShardWithNode(id);
            if (!id.equals(victim) && now != null && (now.id.equals(split[0].id) || now.id.equals(split[1].id)))
                rehomed = id;
        }
        assertNotNull(rehomed);

        Message stayerMsg = applyRemoval(stayer, ops, victim);
        assertTrue(stayerMsg instanceof SplitShard, "shard that split should get SplitShard, got " + stayerMsg);
        SplitShard ss = (SplitShard) stayerMsg;
        assertEquals(split[0].id, ss.inShardId);
        assertEquals(split[1].id, ss.newShardId);
        assertEquals(6, ss.nodes.size() + ss.newNodes.size());
        assertTrue(ss.nodes.containsKey(rehomed) || ss.newNodes.containsKey(rehomed));

        Message movedMsg = applyRemoval(rehomed, ops, victim);
        assertTrue(movedMsg instanceof Update && !(movedMsg instanceof SplitShard),
            "re-homed node should get a shard removal Update, got " + movedMsg);
        Update u = (Update) movedMsg;
        assertTrue(u.shardRemoved);
        assertEquals(6, u.nodes.size(), "re-homed node should know both halves of the splitting shard");
        assertNotNull(u.ring);
    }

    /** Applies the history then the removal on a ClusterState for nodeId and returns the shard message it queued. */
    private static Message applyRemoval(String nodeId, List<Snapshot.MembershipOp> ops, String victim) throws Exception {
        Pipe in = new Pipe();
        ClusterState cs = new ClusterState(in, seeds(), nodeId, new Pipe(), new Pipe(), new Pipe(), new Pipe());
        Snapshot snap = new Snapshot(0, 0, 0, null, null, null, 0);
        snap.membershipOps = ops;
        cs.restore(snap);
        cs.apply(new NodeMsg("Remove", victim, 0));

        Message pending = in.take(100);
        assertNotNull(pending, nodeId + " queued no follow-up");
        Field f = pending.getClass().getDeclaredField("shardUpdate");
        f.setAccessible(true);
        return (Message) f.get(pending);
    }
}
