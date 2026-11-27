package integration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import com.google.gson.Gson;
import cluster.Node;
import endpoints.Server;
import testutil.Nodes;

/**
 * Runs real server processes on localhost for end-to-end tests.
 *
 * Every harness gets its own working directory holding a generated
 * network.config plus the logs/ and snapshots/ directories the servers write,
 * so tests never touch the project's own files and use free ports that cannot
 * clash with a cluster started by hand.
 */
public class ClusterHarness implements AutoCloseable {
    /** Working directory of every server process started by this harness. */
    public final Path dir;

    /** Seed nodes written to network.config, in order. */
    public final List<Node> seeds = new ArrayList<>();

    /** Every node this harness has started, running or not. */
    public final Map<String, Node> nodes = new LinkedHashMap<>();

    /** Port the spawner listens on, as written to network.config. */
    public final int spawnerPort;

    private final Map<String, Process> processes = new LinkedHashMap<>();
    private final int snapshotThreshold;

    /**
     * Writes a network.config with the given number of seed nodes.
     *
     * @param seedCount number of seed nodes
     * @param snapshotThreshold committed entries per snapshot for the servers
     */
    public ClusterHarness(int seedCount, int snapshotThreshold) throws IOException {
        this.dir = Files.createTempDirectory("diststore-it-");
        this.snapshotThreshold = snapshotThreshold;
        this.spawnerPort = Nodes.closedPort();
        StringBuilder config = new StringBuilder("spawner,localhost," + spawnerPort + "\n");
        for (int i = 0; i < seedCount; i++) {
            Node n = new Node("SEED" + i, "localhost", Nodes.closedPort());
            seeds.add(n);
            nodes.put(n.id, n);
            config.append(n.id).append(",localhost,").append(n.port).append("\n");
        }
        Files.writeString(dir.resolve("network.config"), config.toString().trim());
    }

    /** Classpath holding the compiled project classes and Gson. */
    public static String classpath() {
        try {
            String classes = Paths.get(Server.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
            String gson = Paths.get(Gson.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
            return classes + File.pathSeparator + gson;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** The java executable running this test. */
    public static String java() {
        return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    }

    /** Starts every seed node. */
    public void startSeeds() throws IOException {
        for (Node seed : seeds)
            start(seed.id, false);
    }

    /** Starts (or restarts) a known node on its original port. */
    public Node start(String id, boolean learner) throws IOException {
        Node n = nodes.get(id);
        if (n == null) {
            n = new Node(id, "localhost", Nodes.closedPort());
            nodes.put(id, n);
        }
        ProcessBuilder pb = new ProcessBuilder(java(),
            "-Draft.snapshotThreshold=" + snapshotThreshold,
            "-cp", classpath(), "endpoints.Server", id, String.valueOf(n.port), String.valueOf(learner));
        pb.directory(dir.toFile());
        pb.redirectErrorStream(true);
        pb.redirectOutput(dir.resolve(id + ".out").toFile());
        processes.put(id, pb.start());
        return n;
    }

    /** Kills a node's process and waits for it to exit. */
    public void kill(String id) throws InterruptedException {
        Process p = processes.remove(id);
        if (p != null) {
            p.descendants().forEach(ProcessHandle::destroyForcibly);
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
    }

    /** Kills every running node. */
    public void killAll() throws InterruptedException {
        for (String id : new ArrayList<>(processes.keySet()))
            kill(id);
    }

    /** Whether the node's process is running. */
    public boolean isRunning(String id) {
        Process p = processes.get(id);
        return p != null && p.isAlive();
    }

    /** Nodes whose processes are running. */
    public List<Node> running() {
        List<Node> up = new ArrayList<>();
        for (Map.Entry<String, Process> e : processes.entrySet())
            if (e.getValue().isAlive() && nodes.containsKey(e.getKey()))
                up.add(nodes.get(e.getKey()));
        return up;
    }

    /**
     * Starts the spawner from the built jar in this harness's working
     * directory, which launches every seed. Skips the test if the jar has not
     * been built.
     */
    public Process startSpawner() throws IOException {
        Path jar = Path.of("target", "DistStore-1.0-SNAPSHOT-jar-with-dependencies.jar").toAbsolutePath();
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(jar), "run mvn package first to build " + jar);

        // the spawner launches nodes with a relative path to the jar
        Files.createDirectories(dir.resolve("target"));
        Path link = dir.resolve("target").resolve(jar.getFileName());
        if (!Files.exists(link))
            Files.createSymbolicLink(link, jar);

        ProcessBuilder pb = new ProcessBuilder(java(),
            "-Draft.snapshotThreshold=" + snapshotThreshold,
            "-cp", classpath(), "runnable.Spawner");
        pb.directory(dir.toFile());
        pb.redirectErrorStream(true);
        pb.redirectOutput(dir.resolve("spawner.out").toFile());
        Process spawner = pb.start();
        track("spawner", spawner);
        return spawner;
    }

    /** Registers an externally started process (e.g. the spawner) so it is cleaned up. */
    public void track(String id, Process p) {
        processes.put(id, p);
    }

    /** Polls a condition until it holds or the timeout passes. */
    public static boolean waitFor(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean())
                return true;
            Thread.sleep(200);
        }
        return condition.getAsBoolean();
    }

    /** Kills every process and deletes the working directory, unless -Dit.keep=true. */
    @Override
    public void close() throws Exception {
        killAll();
        if (Boolean.getBoolean("it.keep")) {
            System.out.println("Integration test files kept in " + dir);
            return;
        }
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
