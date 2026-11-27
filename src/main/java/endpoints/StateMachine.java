package endpoints;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import communication.HandOff;
import message.Message;
import message.DictMsg;
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

        String json = gson.toJson(new Response(response));
        responder.submit(() -> {
            try {
                HandOff.sendToNode(dictMsg.client, json, this.logPath);
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
        if (!msg.type.equals("DictMsg")) {
            HandOff.writeToFile("store operation failed: unsupported message type " + msg.type, this.logPath);
            return;
        }
        HandOff.writeToFile("Node:" + nodeId + " StateMachine: applying command: " + msg.type, this.logPath);

        DictMsg dictMsg = (DictMsg) msg;
        sendResponse(parseQuery(dictMsg), dictMsg);
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
