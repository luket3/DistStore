package testutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import message.DictMsg;
import message.Message;
import message.Snapshot;
import raft.ReplicatedStateMachine;

/** In-memory key-value state machine that records what Raft did to it. */
public class KvFake implements ReplicatedStateMachine {
    public final Map<String, String> store = new HashMap<>();
    public final List<Message> applied = new ArrayList<>();
    public Snapshot restoredFrom;

    @Override
    public void apply(Message msg) {
        applied.add(msg);
        if (msg instanceof DictMsg) {
            DictMsg d = (DictMsg) msg;
            if (d.action.equals("Put")) store.put(d.key, d.value);
            else if (d.action.equals("Delete")) store.remove(d.key);
        }
    }

    @Override
    public void fillSnapshot(Snapshot snapshot) {
        snapshot.kvStore = new HashMap<>(store);
    }

    @Override
    public void restore(Snapshot snapshot) {
        store.clear();
        store.putAll(snapshot.kvStore);
        restoredFrom = snapshot;
    }

    @Override
    public Map<String, String> exportKeys() {
        return new HashMap<>(store);
    }

    @Override
    public void retainKeys(Predicate<String> keep) {
        store.keySet().removeIf(k -> !keep.test(k));
    }
}
