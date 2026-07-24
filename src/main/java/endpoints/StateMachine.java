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
import communication.Pipe;

import message.Message;
import message.DictMsg;
import message.Reply;

import com.google.gson.Gson;

/**
 * Per-connection server worker that executes simple key-value queries.
 *
 * <p>Supported queries: {@code Get key}, {@code Put key value},
 * and {@code Delete key}.</p>
 */
public class StateMachine implements Runnable {
    /** Shared in-memory key-value store. */
    private static HashMap<String,String> store = new HashMap<>();
    Comm comm;
    Pipe inPipe;
    String nodeId;
    Gson gson;

    HashMap<Integer, Comm> clientInfo = new HashMap<>();
    int returnCode;


    StateMachine(Pipe inPipe, String nodeId) throws Exception {
        this.returnCode = 0;
        this.inPipe = inPipe;
        this.nodeId = nodeId;
        this.comm = new Comm();
        this.gson = new Gson();
    }

    private void sendResponse(String response) throws Exception {
        Comm client = clientInfo.get(returnCode);
        if (client == null) {
            return;
        }

        System.out.println("Node:" + nodeId + " StateMachine: sending response:" + response);
        client.sendString(response);
        client.closeSocket();
        clientInfo.remove(returnCode);
    }

    // update to fit new logic
    // send message wait for ack then send next message

    // sort data, start at beginning, then send data at index response.index + 1
    // when recieved ack for last data send a message to all nodes indicating that data is redistributed and phase can be updated
    // this message is UpdateNodes message with action = "Finalise"
    // remember timeout and try next node in shard if no response

    // what happens if node dies between getting data and propogating to raft?????
    // could send to all nodes in shard, only leader commits them????

    /**
     * Execute a simple key-value query against the shared store.
     *
     * @param query textual query to execute
     * @return result string or "null" when no value exists / invalid query
     */
    public String parseQuery(Message query) {
        String res;

        // Handle Reply messages by storing the client Comm for later response sending
        if (query.type.equals("Reply")) {
            Reply reply = (Reply) query;
            clientInfo.put(reply.reply_num, reply.comm);
            return "no response";
        }

        // Only process DictMsg queries in store
        if (!query.type.equals("DictMsg")) {
            return "store operation failed: unsupported message type " + query.type;
        }

        // Process DictMsg queries and execute the corresponding store operation
        DictMsg dictMsg = (DictMsg) query;
        returnCode = dictMsg.reply_num;
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
                System.out.println("Node:" + nodeId + " StateMachine: recieved command:");

                String response = parseQuery(query);

                if (!response.equals("no response")) {
                    sendResponse(response);
                }
            }
            catch(Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
