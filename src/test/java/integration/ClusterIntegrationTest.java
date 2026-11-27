package integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import cluster.ConsistentHashMap;
import cluster.Node;
import message.Ack;

/**
 * End-to-end tests that run real server processes and drive them over TCP
 * with the client protocol.
 *
 * Excluded from the normal build because each test boots a cluster and waits
 * for elections. Run with {@code make integration}. Pass {@code -Dit.keep=true}
 * to keep each cluster's working directory (logs, snapshots, process output)
 * for debugging.
 */
@Tag("integration")
class ClusterIntegrationTest {

    /** Writes keys k0..k(n-1) with values v0..v(n-1). */
    private static Map<String, String> writeKeys(TestClient client, String prefix, int n) throws Exception {
        Map<String, String> written = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            client.put(prefix + i, "v" + i);
            written.put(prefix + i, "v" + i);
        }
        return written;
    }

    private static void assertAllReadable(TestClient client, Map<String, String> expected) throws Exception {
        for (Map.Entry<String, String> e : expected.entrySet())
            assertEquals(e.getValue(), client.get(e.getKey()), "value of " + e.getKey());
    }

    @Test
    @Timeout(120)
    void bootsAndServesReadsWritesAndDeletes() throws Exception {
        try (ClusterHarness cluster = new ClusterHarness(3, 100); TestClient client = new TestClient(cluster)) {
            cluster.startSeeds();
            client.refreshConfig(30_000);
            assertEquals(3, client.map.getAllNodes().size());
            assertEquals(0, client.version);

            assertEquals("null", client.put("a", "1"));
            assertEquals("1", client.get("a"));
            assertEquals("1", client.put("a", "2"));
            assertEquals("2", client.delete("a"));
            assertEquals("null", client.get("a"));

            assertAllReadable(client, writeKeys(client, "k", 25));
        }
    }

    @Test
    @Timeout(180)
    void keepsServingWhenAMinorityOfNodesFails() throws Exception {
        try (ClusterHarness cluster = new ClusterHarness(3, 100); TestClient client = new TestClient(cluster)) {
            cluster.startSeeds();
            Map<String, String> written = writeKeys(client, "k", 10);

            // whichever node is the leader, two of three can still elect one and commit
            for (Node victim : cluster.seeds) {
                cluster.kill(victim.id);
                client.markDead(victim.id);
                assertAllReadable(client, written);
                client.put("after-" + victim.id, "ok");
                written.put("after-" + victim.id, "ok");

                cluster.start(victim.id, false);
                assertTrue(ClusterHarness.waitFor(() -> cluster.isRunning(victim.id), 10_000));
                client.markAlive(victim.id);
                client.refreshConfig(30_000);
            }
            assertAllReadable(client, written);
        }
    }

    @Test
    @Timeout(180)
    void restartsFromSnapshotsAfterAFullShutdown() throws Exception {
        int threshold = 5;
        try (ClusterHarness cluster = new ClusterHarness(3, threshold); TestClient client = new TestClient(cluster)) {
            cluster.startSeeds();
            Map<String, String> written = writeKeys(client, "k", 22);
            Thread.sleep(2000); // let followers learn the final commit index

            cluster.killAll();
            for (Node seed : cluster.seeds) {
                Path snap = cluster.dir.resolve("snapshots").resolve(seed.id + "-Shard.json");
                assertTrue(Files.exists(snap), "missing snapshot for " + seed.id);
            }

            cluster.startSeeds();
            // only entries up to the last snapshot survive a full shutdown;
            // snapshots are taken every 5 commits, so k0..k19 are covered
            Map<String, String> durable = new LinkedHashMap<>();
            for (int i = 0; i < 20; i++)
                durable.put("k" + i, written.get("k" + i));
            assertAllReadable(client, durable);

            // and the restored cluster accepts new writes
            assertEquals("null", client.put("fresh", "x"));
            assertEquals("x", client.get("fresh"));
        }
    }

    @Test
    @Timeout(240)
    void addedNodeJoinsAsAVoterWithTheData() throws Exception {
        try (ClusterHarness cluster = new ClusterHarness(3, 100); TestClient client = new TestClient(cluster)) {
            cluster.startSeeds();
            Map<String, String> written = writeKeys(client, "k", 10);

            Node added = cluster.start("N3", true);
            assertEquals("Cluster successfully updated", client.changeMembership("Add", added, 90_000));
            assertEquals(1, client.version);
            assertTrue(client.map.getAllNodes().containsKey("N3"));
            assertAllReadable(client, written);

            // with 4 voters a majority is 3: losing a seed only works if N3 is a caught-up voter
            Thread.sleep(3000);
            cluster.kill("SEED0");
            client.markDead("SEED0");
            assertAllReadable(client, written);
            client.put("after", "ok");
            assertEquals("ok", client.get("after"));
        }
    }

    @Test
    @Timeout(240)
    void removedNodeLeavesTheCluster() throws Exception {
        try (ClusterHarness cluster = new ClusterHarness(4, 100); TestClient client = new TestClient(cluster)) {
            cluster.startSeeds();
            Map<String, String> written = writeKeys(client, "k", 10);

            // the spawner kills the node first, then the client asks the cluster to drop it
            cluster.kill("SEED3");
            client.markDead("SEED3");
            assertEquals("Cluster successfully updated",
                client.changeMembership("Remove", new Node("SEED3", "localhost", -1), 90_000));

            assertEquals(1, client.version);
            assertEquals(3, client.map.getAllNodes().size());
            assertFalse(client.map.getAllNodes().containsKey("SEED3"));
            assertAllReadable(client, written);
            client.put("after", "ok");
            assertEquals("ok", client.get("after"));
        }
    }

    @Test
    @Timeout(600)
    void shardSplitAndMergeKeepEveryKeyInTheRightShard() throws Exception {
        try (ClusterHarness cluster = new ClusterHarness(3, 100); TestClient client = new TestClient(cluster)) {
            cluster.startSeeds();
            Map<String, String> written = writeKeys(client, "k", 40);

            // the sixth node splits the only shard in two
            for (int i = 3; i < 6; i++) {
                Node added = cluster.start("N" + i, true);
                assertEquals("Cluster successfully updated", client.changeMembership("Add", added, 120_000));
            }
            assertEquals(2, client.shardCount());
            assertEquals(6, client.map.getAllNodes().size());

            // every key is still readable through routing...
            assertAllReadable(client, written);
            // ...and each shard dropped the keys it no longer owns
            Map<String, Map<String, Node>> shards = client.map.shardMembers();
            int checked = 0;
            for (String key : written.keySet()) {
                String owner = ConsistentHashMap.ownerOf(client.map.ringOwners(), key);
                for (Map.Entry<String, Map<String, Node>> shard : shards.entrySet()) {
                    if (shard.getKey().equals(owner))
                        continue;
                    for (Node n : shard.getValue().values()) {
                        String answer = client.getFrom(n, key, 3000);
                        if (answer != null) {
                            assertEquals("null", answer, key + " still held by non-owner shard " + shard.getKey());
                            checked++;
                            break;
                        }
                    }
                }
            }
            assertTrue(checked > 0, "no non-owner shard answered, so key placement was not checked");

            // new writes land in both shards
            Map<String, String> more = writeKeys(client, "post-split-", 20);
            written.putAll(more);
            assertAllReadable(client, written);

            // removing a node from a 3-node shard merges it into the other shard
            String victim = shards.values().iterator().next().keySet().iterator().next();
            cluster.kill(victim);
            client.markDead(victim);
            assertEquals("Cluster successfully updated",
                client.changeMembership("Remove", new Node(victim, "localhost", -1), 120_000));
            assertEquals(1, client.shardCount());
            assertEquals(5, client.map.getAllNodes().size());

            // the removed shard's keys were handed over to the surviving shard
            assertAllReadable(client, written);
        }
    }

    @Test
    @Timeout(180)
    void spawnerLaunchesTheClusterFromNetworkConfig() throws Exception {
        Path jar = Path.of("target", "DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar").toAbsolutePath();
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(jar), "run mvn package first to build " + jar);

        try (ClusterHarness cluster = new ClusterHarness(3, 100); TestClient client = new TestClient(cluster)) {
            // the spawner launches nodes with a relative path to the jar
            Files.createDirectories(cluster.dir.resolve("target"));
            Files.createSymbolicLink(cluster.dir.resolve("target").resolve(jar.getFileName()), jar);

            ProcessBuilder pb = new ProcessBuilder(ClusterHarness.java(), "-cp", ClusterHarness.classpath(), "runnable.Spawner");
            pb.directory(cluster.dir.toFile());
            pb.redirectErrorStream(true);
            pb.redirectOutput(cluster.dir.resolve("spawner.out").toFile());
            Process spawner = pb.start();
            cluster.track("spawner", spawner);

            // the harness did not start the seeds itself; reach them through their configured ports
            client.refreshConfig(60_000);
            assertEquals(3, client.map.getAllNodes().size());
            assertEquals("null", client.put("spawned", "yes"));
            assertEquals("yes", client.get("spawned"));
            assertTrue(Files.readString(cluster.dir.resolve("spawner.out")).contains("Creating new Node id:SEED0"));
        }
    }

    /**
     * Kill and Revive through the spawner stop and restart a member's process
     * without a membership change. Each revived node is then needed for a
     * majority, which shows it rejoined as a caught-up voter.
     */
    @Test
    @Timeout(300)
    void spawnerKillAndReviveLeaveMembershipUnchanged() throws Exception {
        // a small snapshot threshold makes revived nodes restore from a snapshot, then catch up
        try (ClusterHarness cluster = new ClusterHarness(3, 5); TestClient client = new TestClient(cluster)) {
            cluster.startSpawner();
            client.refreshConfig(60_000);
            Map<String, String> written = writeKeys(client, "k", 12);

            Ack added = client.spawner("Add", "N3", 30_000);
            assertTrue(added.success, added.message);
            assertEquals("Cluster successfully updated", client.changeMembership("Add", added.node, 90_000));
            assertEquals(1, client.version);

            for (String id : new String[] {"N3", "SEED0"}) {
                // killing a node leaves it in the cluster
                Ack killed = client.spawner("Kill", id, 30_000);
                assertTrue(killed.success, killed.message);
                client.markDead(id);
                client.refreshConfig(30_000);
                assertEquals(1, client.version, "Kill must not change the membership");
                assertTrue(client.map.getAllNodes().containsKey(id));
                assertFalse(client.spawner("Kill", id, 10_000).success);

                // 3 of 4 voters are still up
                client.put("while-" + id + "-down", "ok");
                written.put("while-" + id + "-down", "ok");

                Ack revived = client.spawner("Revive", id, 30_000);
                assertTrue(revived.success, revived.message);
                assertEquals(cluster.nodes.get(id).port, revived.node.port, "revived on its original port");
                assertFalse(client.spawner("Revive", id, 10_000).success);
                client.markAlive(id);
                Thread.sleep(3000); // let it catch up

                // with another node down, a majority (3 of 4) needs the revived node
                String other = id.equals("N3") ? "SEED1" : "SEED2";
                assertTrue(client.spawner("Kill", other, 30_000).success);
                client.markDead(other);
                assertAllReadable(client, written);
                client.put("after-" + id + "-revived", "ok");
                written.put("after-" + id + "-revived", "ok");

                assertTrue(client.spawner("Revive", other, 30_000).success);
                client.markAlive(other);
                Thread.sleep(3000);
            }

            client.refreshConfig(30_000);
            assertEquals(1, client.version);
            assertEquals(4, client.map.getAllNodes().size());
            assertAllReadable(client, written);
        }
    }
}
