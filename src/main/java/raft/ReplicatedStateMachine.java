package raft;

import java.util.Map;
import java.util.function.Predicate;
import message.Message;
import message.Snapshot;

/**
 * State machine driven by a Raft log.
 *
 * Raft calls every method on its own thread, in log order, so the state
 * machine's state always matches the log's commit index exactly. This is what
 * makes a snapshot taken at index N contain exactly the effects of entries up
 * to N. Implementations must not block: any slow work (network replies,
 * waiting on other Raft groups) has to be handed off to another thread.
 */
public interface ReplicatedStateMachine {

    /**
     * Applies one committed client or membership message.
     *
     * @param msg committed log payload (DictMsg or NodeMsg)
     */
    void apply(Message msg);

    /**
     * Copies the state machine's current state into a snapshot being built.
     *
     * @param snapshot snapshot to fill
     */
    void fillSnapshot(Snapshot snapshot);

    /**
     * Replaces the state machine's state with the contents of a snapshot.
     *
     * @param snapshot snapshot restored from disk or installed from the leader
     */
    void restore(Snapshot snapshot);

    /**
     * Returns a copy of every key/value pair held, so it can be handed to
     * other shards. State machines that hold no key/value data return an
     * empty map.
     *
     * @return copy of the key/value data
     */
    default Map<String, String> exportKeys() {
        return Map.of();
    }

    /**
     * Drops every key the predicate rejects. Used when a shard splits (each
     * half keeps only the keys it owns) and when a node leaves a removed shard
     * (it drops everything before catching up with its new shard).
     *
     * @param keep returns true for keys to keep
     */
    default void retainKeys(Predicate<String> keep) {
    }
}
