package message;

import cluster.Node;;

public class Reply extends Message {
    public Node client;

    public Reply(String type, int version, Node client) {
        super(type, version);
        this.client = client;
    }

    public Reply(String type, int version) {
        super(type, version);
        this.client = null;
    }
    
}
