package endpoints;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;
import message.DictMsg;
import message.MigrateData;

class StateMachineTest {

    @Test
    void migrationNeverOverwritesNewerValues() {
        StateMachine sm = new StateMachine("smTest");
        sm.apply(new DictMsg("Put", "a", "new", 0));
        sm.apply(new MigrateData("m1", "shard1", Map.of("a", "old", "b", "moved"), null));
        sm.apply(new MigrateData("m1", "shard1", Map.of("a", "old", "b", "moved"), null)); // retry duplicate

        assertEquals(Map.of("a", "new", "b", "moved"), sm.exportKeys());

        sm.retainKeys(k -> k.equals("b"));
        assertEquals(Map.of("b", "moved"), sm.exportKeys());
    }
}
