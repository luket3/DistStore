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
import message.MessageInfo;

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

    HashMap<String, Comm> clientInfo = new HashMap<>();
    String returnCode;


    StateMachine(Pipe pipe, String nodeId) throws Exception {
        this.returnCode = "0";
        this.inPipe = pipe;
        this.nodeId = nodeId;
        this.comm = new Comm();
    }

    private void sendResponse(String response) throws Exception {
        Comm client = clientInfo.get(returnCode);
        if (client == null) {
            return;
        }

        System.out.println("Node:" + nodeId + " sending response:" + response);
        client.sendString(response);
        client.closeSocket();
        clientInfo.remove(returnCode);
    }

    /**
     * Execute a simple key-value query against the shared store.
     *
     * @param query textual query to execute
     * @return result string or "null" when no value exists / invalid query
     */
    public String parseQuery(MessageInfo query) {
        String[] split = query.message.split(" ");
        String res;
        
        returnCode = split[0];
        String messageType = split[1];

        if (messageType.equals("Reply")) {
            clientInfo.put(returnCode, query.comm);
            return "no response";
        }

        if (messageType.equals("Get"))
            res = store.get(split[2]);
        else if (messageType.equals("Put"))
            res = store.put(split[2], split[3]);
        else if (messageType.equals("Delete"))
            res = store.remove(split[2]);
        else
            res = "null";

        if (res == null)
            res = "null";
        return res;
    }

    @Override
    public void run() {

        while (true) {
            try {
                MessageInfo query = inPipe.takeAll();
                String logMessage = "Node:" + nodeId + " recieved command:"
                        + query.message;
                System.out.println(logMessage);

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
