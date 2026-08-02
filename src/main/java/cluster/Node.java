package cluster;

/**
 * Lightweight representation of a cluster node in the distributed key-value store.
 * This class holds the essential information needed to identify and communicate
 * with a node in the cluster.
 */
public class Node {

    /** Node identifier (e.g. "N1"). */
    public String id;

    /** IP address or hostname of the node. */
    public String ip;

    /** TCP port the node listens on. */
    public int port;

    /**
     * Constructs a new Node with the specified identifier, IP address, and port.
     *
     * @param id the node identifier (e.g., "N1")
     * @param ip the node's IP address or hostname
     * @param port the TCP port the node listens on
     */
    public Node(String id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    /**
     * Prints a single-line representation of this node to standard output.
     * Format: <node_id> <ip_address> <port>
     */
    void print() {
        System.out.println(id + " " + ip + " " + port);
    }
}