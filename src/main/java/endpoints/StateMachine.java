package endpoints;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import communication.HandOff;
import message.Message;
import message.DictMsg;
import message.MigrateAck;
import message.MigrateData;
import message.Response;
import message.Snapshot;
import raft.ReplicatedStateMachine;
import com.google.gson.Gson;

/**
 * Local command executor for a single node's replicated key-value store.
 *
 * The shard Raft applies each committed dictionary operation synchronously on
 * its own thread, so the store always reflects exactly the committed log
 * prefix and can be snapshotted directly. Sending the result back to the
 * client is network I/O, so it is handed to a background responder thread and
 * never blocks the Raft loop.
 */
public class StateMachine implements ReplicatedStateMachine {
    /** Key-value table holding the committed state. Only touched by the shard Raft thread. */
    private final HashMap<String,String> store;

    /** Background thread used to deliver client responses. */
    private final ExecutorService responder;

    /** Node identifier*/
    private final String nodeId;

    /** Gson instance for JSON serialization and deserialization */
    private final Gson gson;

    /** Path to the log file for recording state machine operations */
    private final String logPath;

    /**
     * Constructs a new StateMachine for the specified node.
     *
     * @param nodeId the identifier of this node
     */
    StateMachine(String nodeId) {
        this.store = new HashMap<>();
        this.nodeId = nodeId;
        this.gson = new Gson();
        this.logPath = "logs/StateMachine.log";
        this.responder = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "StateMachine-responder-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Sends the textual result of a completed command back to the client that
     * originated the request, on the responder thread.
     *
     * @param response result text produced by the command execution
     * @param dictMsg request message whose client address is used as the
     *     response destination
     */
    private void sendResponse(String response, DictMsg dictMsg) {
        if (dictMsg.client == null)
            return;
        send(dictMsg.client, gson.toJson(new Response(response)));
    }

    /**
     * Delivers a serialized message on the responder thread.
     *
     * @param target node to deliver to
     * @param json serialized message
     */
    private void send(cluster.Node target, String json) {
        responder.submit(() -> {
            try {
                HandOff.sendToNode(target, json, this.logPath);
            } catch (Exception e) {
                HandOff.writeToFile("Node:" + nodeId + " StateMachine: failed to send response: " + e.getMessage(), this.logPath);
            }
        });
    }

    /**
     * Executes a dictionary operation against the store.
     *
     * Supported actions are Get, Put, and Delete. Unsupported operations are
     * reported as a textual failure message. A missing lookup result is
     * converted to the string "null" so callers can distinguish an empty
     * value from a malformed operation.
     *
     * @param dictMsg message containing the key-value command to execute
     * @return textual result of the operation
     */
    public String parseQuery(DictMsg dictMsg) {
        String res;
        if (dictMsg.action.equals("Get"))
            res = store.get(dictMsg.key);
        else if (dictMsg.action.equals("Put"))
            res = store.put(dictMsg.key, dictMsg.value);
        else if (dictMsg.action.equals("Delete"))
            res = store.remove(dictMsg.key);
        else
            res = "store operation failed: unsupported action " + dictMsg.action;

        if (res == null)
            res = "null";
        return res;
    }

    /**
     * Applies a committed command and queues the reply to its client.
     *
     * @param msg committed log payload; only DictMsg is executed
     */
    @Override
    public void apply(Message msg) {
        if (msg.type.equals("MigrateData")) {
            applyMigration((MigrateData) msg);
            return;
        }
        if (!msg.type.equals("DictMsg")) {
            HandOff.writeToFile("store operation failed: unsupported message type " + msg.type, this.logPath);
            return;
        }
        HandOff.writeToFile("Node:" + nodeId + " StateMachine: applying command: " + msg.type, this.logPath);

        DictMsg dictMsg = (DictMsg) msg;
        sendResponse(parseQuery(dictMsg), dictMsg);
    }

    /**
     * Stores the key/value pairs handed over from a removed shard. A key that
     * already exists is left alone: its value was written to this shard after
     * the hand-over started, so it is newer. Acknowledges the sender when this
     * node is the leader (followers have the sender cleared by the Raft log).
     *
     * @param migration committed hand-over from a removed shard
     */
    private void applyMigration(MigrateData migration) {
        int added = 0;
        if (migration.entries != null) {
            for (Map.Entry<String, String> e : migration.entries.entrySet()) {
                if (store.putIfAbsent(e.getKey(), e.getValue()) == null)
                    added++;
            }
        }
        HandOff.writeToFile("Node:" + nodeId + " StateMachine: applied migration " + migration.id
            + ", added " + added + " of " + (migration.entries == null ? 0 : migration.entries.size()) + " keys", this.logPath);

        if (migration.client != null)
            send(migration.client, gson.toJson(new MigrateAck(migration.id)));
    }

    /**
     * Returns a copy of the store.
     *
     * @return copy of every key/value pair
     */
    @Override
    public Map<String, String> exportKeys() {
        return new HashMap<>(store);
    }

    /**
     * Drops every key the predicate rejects.
     *
     * @param keep returns true for keys to keep
     */
    @Override
    public void retainKeys(Predicate<String> keep) {
        int before = store.size();
        store.keySet().removeIf(key -> !keep.test(key));
        HandOff.writeToFile("Node:" + nodeId + " StateMachine: kept " + store.size() + " of " + before + " keys", this.logPath);
    }

    /**
     * Copies the current store into a snapshot.
     *
     * @param snapshot snapshot being built by the shard Raft log
     */
    @Override
    public void fillSnapshot(Snapshot snapshot) {
        snapshot.kvStore = new HashMap<>(store);
    }

    /**
     * Replaces the store with a snapshot's contents.
     *
     * @param snapshot snapshot restored from disk or installed from the leader
     */
    @Override
    public void restore(Snapshot snapshot) {
        store.clear();
        if (snapshot.kvStore != null)
            store.putAll(snapshot.kvStore);
        HandOff.writeToFile("Node:" + nodeId + " StateMachine: loaded snapshot up to index "
            + snapshot.lastIncludedIndex + " with " + store.size() + " keys", this.logPath);
    }
}
