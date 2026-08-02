package endpoints;

/*
 * File: Server.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Per-connection server worker that executes simple
 * key-value queries.
 */

import java.util.HashMap;

import communication.Comm;
import communication.HandOff;
import communication.Pipe;

import message.Message;
import message.DictMsg;
import message.Response;

import com.google.gson.Gson;

/**
 * Local application worker that executes a single in-memory key-value store
 * for the node that receives the committed Raft operation.
 *
 * This implementation is not yet a fully shard-aware distributed state
 * machine. It currently maintains one shared process-local {@link HashMap}
 * and applies every supported client operation to that local table.
 */
public class StateMachine implements Runnable {
    /**
     * Single process-local key-value table used by this node to apply
     * committed client operations.
     */
    private static HashMap<String,String> store = new HashMap<>();
    Comm comm;
    Pipe inPipe;
    String nodeId;
    Gson gson;
    String logPath;


    StateMachine(Pipe inPipe, String nodeId) throws Exception {
        this.inPipe = inPipe;
        this.nodeId = nodeId;
        this.comm = new Comm();
        this.gson = new Gson();
        this.logPath = "logs/StateMachine.log";
    }

    private void sendResponse(String response, Message query) throws Exception {
        DictMsg dictMsg = (DictMsg) query;

        if (dictMsg.client == null)
            return;
        
        Response clientResponse = new Response(response);
        HandOff.sendToNode(dictMsg.client, gson.toJson(clientResponse), this.logPath);
    }

    /**
     * Execute a simple key-value query against the shared store.
     *
     * @param query textual query to execute
     * @return result string or "null" when no value exists / invalid query
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

    @Override
    public void run() {

        while (true) {
            try {
                Message query = inPipe.take();
                // Only process DictMsg queries in store
                if (!query.type.equals("DictMsg")) {
                    HandOff.writeToFile("store operation failed: unsupported message type " + query.type, this.logPath);
                    continue;
                } else {
                    HandOff.writeToFile("Node:" + nodeId + " StateMachine: recieved command: " + query.type, this.logPath);
                }

                String response = parseQuery(query);
                sendResponse(response, query);
            }
            catch(Exception e) {
                HandOff.writeToFile(e.getMessage(), this.logPath);
            }
        }
    }
}
