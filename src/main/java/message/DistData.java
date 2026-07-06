package message;

import cluster.ConsistentHashMap;

public class DistData extends Message{
    
    public ConsistentHashMap cluster;
    public int version;

    public DistData(ConsistentHashMap cluster, int version) {
        super("DistData");
        this.cluster = cluster;
        this.version = version;
    }
}
