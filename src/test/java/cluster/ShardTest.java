package cluster;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ShardTest {

    private static Shard shardWith(int count) {
        Shard s = new Shard("s", 3);
        for (int i = 0; i < count; i++)
            s.addNode(new Node("N" + i, "localhost", 1000 + i));
        return s;
    }

    @Test
    void fillsLeftThenRight() {
        Shard s = shardWith(5);
        assertEquals(5, s.size());
        assertEquals(3, s.getLeft().size());
        assertEquals(2, s.getRight().size());
        assertEquals("N3", s.getRight().get(0).id);
        assertTrue(s.contains("N0"));
        assertTrue(s.contains("N4"));
        assertFalse(s.contains("N9"));
        assertEquals(5, s.getAllNodes().size());
    }

    @Test
    void splitsOnlyAtTwiceMinimumSize() {
        assertNull(shardWith(5).split("t"));

        Shard s = shardWith(6);
        Shard split = s.split("t");
        assertNotNull(split);
        assertEquals("t", split.id);
        assertEquals(3, split.size());
        assertEquals(Set.of("N3", "N4", "N5"), split.getAllNodes().keySet());
        assertEquals(3, s.size());
        assertEquals(Set.of("N0", "N1", "N2"), s.getAllNodes().keySet());
        assertTrue(s.getRight().isEmpty());
    }

    @Test
    void removingFromLeftRefillsFromRight() {
        Shard s = shardWith(4);
        s.removeNode("N1");
        assertEquals(3, s.size());
        assertEquals(3, s.getLeft().size());
        assertTrue(s.getRight().isEmpty());
        assertFalse(s.contains("N1"));
    }

    @Test
    void removingFromRight() {
        Shard s = shardWith(5);
        s.removeNode("N4");
        assertEquals(4, s.size());
        assertFalse(s.contains("N4"));
        assertEquals(3, s.getLeft().size());
    }

    @Test
    void getSkipsKilledNodes() {
        Shard s = shardWith(4);
        assertEquals("N0", s.get(null).id);
        assertEquals("N1", s.get(new HashSet<>(Set.of("N0"))).id);
        assertEquals("N3", s.get(new HashSet<>(Set.of("N0", "N1", "N2"))).id);
        assertNull(s.get(new HashSet<>(Set.of("N0", "N1", "N2", "N3"))));
    }
}
