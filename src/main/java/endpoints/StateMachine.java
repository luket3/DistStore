package endpoints;


import java.util.HashMap;
import communication.HandOff;
import communication.Pipe;
import message.Message;
import message.DictMsg;
import message.Response;
import com.google.gson.Gson;

/**
 * Local command executor for a single node's replicated key-value store.
 *
 * This worker receives committed dictionary operations from the shard Raft
 * pipeline, applies each operation to a shared process-local in-memory map,
 * and sends the resulting textual outcome back to the originating client.
 */
public class StateMachine implements Runnable {
    /**
     * Shared process-local key-value table used by this node to apply
     * committed client operations.
     */
    private static HashMap<String,String> store = new HashMap<>();

    /** The pipe for receiving incoming messages */
    private Pipe inPipe;

    /** Node identifier*/
    private String nodeId;

    /** Gson instance for JSON serialization and deserialization */
    private Gson gson;

    /** Path to the log file for recording state machine operations */
    private String logPath;


    /**
     * Constructs a new StateMachine instance with the specified input pipe and
     * node identifier.
     *
     * @param inPipe the pipe for receiving incoming messages
     * @param nodeId the identifier of this node
     * @throws Exception if an error occurs during initialization
     */
    StateMachine(Pipe inPipe, String nodeId) throws Exception {
        this.inPipe = inPipe;
        this.nodeId = nodeId;
        this.gson = new Gson();
        this.logPath = "logs/StateMachine.log";
    }

    /**
     * Sends the textual result of a completed command back to the client that
     * originated the request.
     *
     * @param response result text produced by the command execution
     * @param query request message whose client address is used as the response
     *     destination
     * @throws Exception if the response cannot be serialized or delivered
     */
    private void sendResponse(String response, Message query) throws Exception {
        DictMsg dictMsg = (DictMsg) query;

        if (dictMsg.client == null)
            return;

        Response clientResponse = new Response(response);
        HandOff.sendToNode(dictMsg.client, gson.toJson(clientResponse), this.logPath);
    }

    /**
     * Executes a dictionary operation against the shared in-memory store.
     *
     * Supported actions are Get, Put, and Delete. Unsupported operations are
     * reported as a textual failure message. A missing lookup result is
     * converted to the string "null" so callers can distinguish an empty
     * value from a malformed operation.
     *
     * @param query message containing the key-value command to execute
     * @return textual result of the operation
     */
    public String parseQuery(Message query) {
        String res;

        // Process DictMsg queries and execute the corresponding store operation
        DictMsg dictMsg = (DictMsg) query;
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
     * Runs the service loop that continuously consumes committed commands.
     *
     * Each received message is validated as a dictionary command, executed
     * against the local store, and then returned to the client using the
     * origin node recorded in the request.
     */
    @Override
    public void run() {

        while (true) {
            try {
                // Wait for the next committed message from the shard Raft pipeline,
                // only execute "DictMsg" messages
                Message query = inPipe.take();
                if (!query.type.equals("DictMsg")) {
                    HandOff.writeToFile("store operation failed: unsupported message type " + query.type, this.logPath);
                    continue;
                } else {
                    HandOff.writeToFile("Node:" + nodeId + " StateMachine: recieved command: " + query.type, this.logPath);
                }

                // Execute the command and send the result back to the client
                String response = parseQuery(query);
                sendResponse(response, query);
            }
            catch(Exception e) {
                HandOff.writeToFile(e.getMessage(), this.logPath);
            }
        }
    }
}
