package cluster;

/*
 * File: Node.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Lightweight representation of a cluster node.
 */

/**
 * Lightweight representation of a cluster node.
 */
public class Node {

    /** Node identifier (e.g. "N1"). */
    public String id; // node identifier

    /** IP address or hostname of the node. */
    public String ip; // ip of node

    /** TCP port the node listens on. */
    public int port; // port of node

    /**
     * Construct a {@code Node} with the supplied id, ip and port.
     *
     * @param id node identifier
     * @param ip node IP or hostname
     * @param port node TCP port
     */
    public Node(String id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    /** Print a single-line representation of this node to stdout. */
    void print() {
        System.out.println(id + " " + ip + " " + port);
    }
}