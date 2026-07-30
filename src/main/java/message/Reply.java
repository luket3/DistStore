package message;

import cluster.Node;;

public class Reply extends Message {
    public Node client;
    public int version;

    public Reply(String type, int version, Node client) {
        super(type);
        this.client = client;
        this.version = version;
    }

    public Reply(String type, int version) {
        super(type);
        this.client = null;
        this.version = version;
    }
    
}
