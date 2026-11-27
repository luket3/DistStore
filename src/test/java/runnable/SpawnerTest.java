package runnable;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import cluster.Node;
import com.google.gson.Gson;
import message.Snapshot;

class SpawnerTest {

    private final Path older = Paths.get(Spawner.SNAPSHOT_DIR, "spawnTestOld-Cluster.json");
    private final Path newer = Paths.get(Spawner.SNAPSHOT_DIR, "spawnTestNew-Cluster.json");

    @AfterEach
    void cleanup() throws Exception {
        Files.deleteIfExists(older);
        Files.deleteIfExists(newer);
    }

    private void write(Path path, int index, Snapshot.MembershipOp... ops) throws Exception {
        Snapshot snap = new Snapshot(index, 1, 1, null, null, null, 0);
        snap.membershipOps = List.of(ops);
        Files.createDirectories(path.getParent());
        Files.writeString(path, new Gson().toJson(snap));
    }

    @Test
    void replaysNewestClusterSnapshotOverSeeds() throws Exception {
        Spawner.gson = new Gson();
        List<Node> seeds = List.of(
            new Node("S0", "localhost", 4567),
            new Node("S1", "localhost", 4568));

        write(older, 10, new Snapshot.MembershipOp("Add", new Node("stale", "localhost", 9999)));
        write(newer, 250,
            new Snapshot.MembershipOp("Add", new Node("N1", "localhost", 4570)),
            new Snapshot.MembershipOp("Add", new Node("N2", "localhost", 4571)),
            new Snapshot.MembershipOp("Remove", new Node("S1", null, -1)));

        Map<String, Node> members = Spawner.currentMembership(seeds);
        assertEquals(List.of("S0", "N1", "N2"), List.copyOf(members.keySet()));
        assertEquals(4571, members.get("N2").port);
    }
}
