package testutil;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import cluster.Node;

/** Helpers for building node maps and finding ports. */
public final class Nodes {
    private Nodes() {}

    /** A port nothing is listening on, so sends to it fail immediately. */
    public static int closedPort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** A node nothing is listening on. */
    public static Node unreachable(String id) {
        return new Node(id, "localhost", closedPort());
    }

    /** Builds a node map from the given nodes. */
    public static Map<String, Node> map(Node... nodes) {
        Map<String, Node> m = new HashMap<>();
        for (Node n : nodes)
            m.put(n.id, n);
        return m;
    }

    /** Deletes the snapshot files a node may have written. */
    public static void deleteSnapshots(String... ids) {
        for (String id : ids) {
            for (String level : new String[] {"Shard", "Cluster"}) {
                try {
                    Files.deleteIfExists(Paths.get("snapshots", id + "-" + level + ".json"));
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
