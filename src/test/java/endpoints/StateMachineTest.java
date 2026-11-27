package endpoints;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import message.DictMsg;
import message.MigrateAck;
import message.MigrateData;
import message.NodeMsg;
import message.Response;
import message.Snapshot;
import testutil.Capture;

class StateMachineTest {

    private StateMachine sm;
    private Capture capture;

    @BeforeEach
    void setUp() throws Exception {
        sm = new StateMachine("smTest");
        capture = new Capture();
    }

    @AfterEach
    void tearDown() throws Exception {
        capture.close();
    }

    @Test
    void executesGetPutDelete() {
        assertEquals("null", sm.parseQuery(new DictMsg("Put", "a", "1", 0)));
        assertEquals("1", sm.parseQuery(new DictMsg("Put", "a", "2", 0)));   // returns previous value
        assertEquals("2", sm.parseQuery(new DictMsg("Get", "a", null, 0)));
        assertEquals("2", sm.parseQuery(new DictMsg("Delete", "a", null, 0)));
        assertEquals("null", sm.parseQuery(new DictMsg("Get", "a", null, 0)));
        assertTrue(sm.parseQuery(new DictMsg("Frobnicate", "a", null, 0)).contains("unsupported action"));
    }

    @Test
    void repliesToTheClientAfterApplying() throws Exception {
        sm.apply(new DictMsg("Put", "a", "1", 0, capture.node("client")));
        sm.apply(new DictMsg("Get", "a", null, 0, capture.node("client")));
        assertEquals("null", capture.next(Response.class, 2000).msg);
        assertEquals("1", capture.next(Response.class, 2000).msg);
    }

    @Test
    void ignoresNonDictMessages() throws Exception {
        sm.apply(new NodeMsg("Add", "N1", 0));
        assertTrue(sm.exportKeys().isEmpty());
        assertNull(capture.next(200));
    }

    @Test
    void migrationNeverOverwritesNewerValuesAndIsAcknowledged() throws Exception {
        sm.apply(new DictMsg("Put", "a", "new", 0));
        sm.apply(new MigrateData("m1", "shard1", Map.of("a", "old", "b", "moved"), capture.node("sender")));
        sm.apply(new MigrateData("m1", "shard1", Map.of("a", "old", "b", "moved"), null)); // retry duplicate

        assertEquals(Map.of("a", "new", "b", "moved"), sm.exportKeys());
        assertEquals("m1", capture.next(MigrateAck.class, 2000).id);
    }

    @Test
    void retainKeysDropsRejectedKeys() {
        sm.apply(new DictMsg("Put", "a", "1", 0));
        sm.apply(new DictMsg("Put", "b", "2", 0));
        sm.retainKeys(k -> k.equals("b"));
        assertEquals(Map.of("b", "2"), sm.exportKeys());
    }

    @Test
    void snapshotsAndRestores() {
        sm.apply(new DictMsg("Put", "a", "1", 0));
        Snapshot snap = new Snapshot(0, 0, 0, null, null, null, 0);
        sm.fillSnapshot(snap);
        assertEquals(Map.of("a", "1"), snap.kvStore);

        sm.apply(new DictMsg("Put", "b", "2", 0));
        sm.restore(snap);
        assertEquals(Map.of("a", "1"), sm.exportKeys());

        // the snapshot is a copy, not a live view
        sm.apply(new DictMsg("Put", "c", "3", 0));
        assertEquals(Map.of("a", "1"), snap.kvStore);
    }
}
