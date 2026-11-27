package integration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import cluster.ConsistentHashMap;
import cluster.Node;
import cluster.Shard;
import communication.HandOff;
import message.Config;
import message.DictMsg;
import message.Message;
import message.Ack;
import message.NodeMsg;
import message.Response;
import testutil.Capture;

/**
 * Client for end-to-end tests that speaks the same protocol as ClientImp:
 * fetch the cluster map, route each key to its shard, retry on timeouts and
 * refresh the map when told the configuration is stale.
 */
public class TestClient implements AutoCloseable {
    private final ClusterHarness cluster;
    private final Capture inbox;
    private final Node self;
    private final Set<String> dead = new HashSet<>();

    /** Latest cluster map received. */
    public ConsistentHashMap map;

    /** Cluster version of the latest map. */
    public int version = -1;

    public TestClient(ClusterHarness cluster) throws Exception {
        this.cluster = cluster;
        this.inbox = new Capture();
        this.self = inbox.node("itClient");
    }

    private boolean send(Node target, Message msg) {
        try {
            HandOff.sendToNode(target, Capture.GSON.toJson(msg), cluster.dir.resolve("client.log").toString());
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Fetches the cluster map from any known node, retrying until the deadline. */
    public void refreshConfig(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Node n : cluster.nodes.values()) {
                if (dead.contains(n.id))
                    continue;
                inbox.clear();
                if (!send(n, new Config(self)))
                    continue;
                Config reply = inbox.next(Config.class, 2000);
                if (reply != null && reply.config != null) {
                    map = reply.config;
                    version = reply.version;
                    return;
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("no node answered a Config request within " + timeoutMs + "ms");
    }

    /** Marks a node as down so requests are not routed to it. */
    public void markDead(String id) {
        dead.add(id);
    }

    /** Marks a restarted node as usable again. */
    public void markAlive(String id) {
        dead.remove(id);
    }

    /** Sends a key-value command to the key's shard, retrying until it is answered. */
    public String request(String action, String key, String value, long timeoutMs) throws Exception {
        if (map == null)
            refreshConfig(timeoutMs);
        long deadline = System.currentTimeMillis() + timeoutMs;
        Set<String> skip = new HashSet<>();
        while (System.currentTimeMillis() < deadline) {
            Shard shard = map.getShard(key);
            Set<String> avoid = new HashSet<>(dead);
            avoid.addAll(skip);
            Node target = shard.get(new HashSet<>(avoid));
            if (target == null) {
                skip.clear();
                target = shard.get(new HashSet<>(dead));
                if (target == null) {
                    refreshConfig(deadline - System.currentTimeMillis());
                    continue;
                }
            }

            inbox.clear();
            if (!send(target, new DictMsg(action, key, value, version, self))) {
                skip.add(target.id);
                Thread.sleep(200);
                continue;
            }
            Response reply = inbox.next(Response.class, 3000);
            if (reply == null) {
                skip.add(target.id); // no leader known there yet, or it is down
                continue;
            }
            if (reply.msg.equals("Invalid config")) {
                refreshConfig(deadline - System.currentTimeMillis());
                continue;
            }
            return reply.msg;
        }
        throw new AssertionError(action + " " + key + " was not answered within " + timeoutMs + "ms");
    }

    public String put(String key, String value) throws Exception {
        return request("Put", key, value, 45_000);
    }

    public String get(String key) throws Exception {
        return request("Get", key, null, 45_000);
    }

    public String delete(String key) throws Exception {
        return request("Delete", key, null, 45_000);
    }

    /**
     * Sends a Get straight to one node, bypassing routing, and returns the
     * answer or null if it does not answer. Used to check a shard does not
     * hold keys it no longer owns.
     */
    public String getFrom(Node target, String key, long timeoutMs) throws Exception {
        inbox.clear();
        if (!send(target, new DictMsg("Get", key, null, version, self)))
            return null;
        Response reply = inbox.next(Response.class, timeoutMs);
        return reply == null ? null : reply.msg;
    }

    /**
     * Asks the cluster to apply a membership change, the way the client does
     * after the spawner has started or killed the node. Like the real client,
     * it retries on another node if no reply arrives: the request can be lost
     * when it is forwarded to a leader that has just died, and the cluster
     * treats a repeated change as a no-op.
     */
    public String changeMembership(String action, Node node, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Set<String> tried = new HashSet<>();
        while (System.currentTimeMillis() < deadline) {
            refreshConfig(30_000);
            Set<String> avoid = new HashSet<>(dead);
            avoid.addAll(tried);
            avoid.add(node.id);
            Node target = map.getShard(node.id).get(new HashSet<>(avoid));
            if (target == null)
                target = cluster.running().stream().filter(n -> !avoid.contains(n.id)).findFirst().orElse(null);
            if (target == null) {
                tried.clear();
                continue;
            }

            inbox.clear();
            if (!send(target, new NodeMsg(action, node, version, self))) {
                tried.add(target.id);
                continue;
            }
            long wait = Math.min(15_000, Math.max(1, deadline - System.currentTimeMillis()));
            Response reply = inbox.next(Response.class, wait);
            if (reply == null) {
                tried.add(target.id);
                continue;
            }
            refreshConfig(30_000);
            return reply.msg;
        }
        throw new AssertionError(action + " " + node.id + " was not confirmed within " + timeoutMs + "ms");
    }

    /**
     * Sends a node-management request to the spawner and returns its
     * acknowledgement, retrying the connection until the spawner is up. A node
     * the spawner launched or revived is recorded in the harness's node list.
     */
    public Ack spawner(String action, String id, long timeoutMs) throws Exception {
        Node spawnerNode = new Node("spawner", "localhost", cluster.spawnerPort);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            inbox.clear();
            if (!send(spawnerNode, new NodeMsg(action, new Node(id, "localhost", -1), version, self))) {
                Thread.sleep(200);
                continue;
            }
            Ack ack = inbox.next(Ack.class, Math.max(1, deadline - System.currentTimeMillis()));
            if (ack != null && ack.success && ack.node != null && ack.node.port > 0)
                cluster.nodes.put(ack.node.id, ack.node);
            return ack;
        }
        throw new AssertionError("spawner did not answer " + action + " " + id + " within " + timeoutMs + "ms");
    }

    /** Number of distinct shards in the current map. */
    public int shardCount() {
        return map.shardMembers().size();
    }

    /** Members of the shard that owns a key in the current map. */
    public Map<String, Node> shardOf(String key) throws Exception {
        return map.getShard(key).getAllNodes();
    }

    @Override
    public void close() throws Exception {
        inbox.close();
    }
}
