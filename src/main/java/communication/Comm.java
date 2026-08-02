package communication;

/*
 * File: Comm.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Lightweight communication helper for reading and writing
 * length-prefixed UTF-8 strings over a TCP Socket.
 * */

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Small socket wrapper used to exchange length-prefixed UTF-8 messages with
 * other nodes in the cluster.
 */
public class Comm {
    /** Underlying TCP socket used for communication. */
    private Socket socket;

    /**
     * Create a {@code Comm} bound to an existing socket.
     *
     * @param socket the already-connected socket
     */
    public Comm(Socket socket) {
        this.socket = socket;
    }

    /**
     * Create an uninitialized {@code Comm}; call {@link #createSocket} to
     * connect.
     */
    public Comm() {}

    /**
     * Close the current socket if open.
     *
     * @throws Exception on socket close errors
     */
    public void closeSocket() throws Exception {
        socket.close();
    }

    /**
     * Create and connect a socket to the provided IP and port.
     *
     * @param ip the remote host IP or hostname
     * @param port the remote TCP port
     * @throws Exception on connection failures
     */
    public void createSocket(String ip, int port) throws Exception {
        socket = new Socket(ip, port);
    }

    /**
     * Read a length-prefixed UTF-8 encoded string from the socket.
     *
     * @return the decoded string
     * @throws Exception on I/O errors
     */
    public String readString() throws Exception {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int length = in.readInt();
        byte[] buffer = new byte[length];

        in.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Send a UTF-8 string prefixed with its 4-byte length.
     *
     * @param message the message to send
     * @throws Exception on I/O errors
     */
    public void sendString(String message) throws Exception
    {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    public String getHostIp() {
        if (socket == null) {
            return null;
        }

        return socket.getInetAddress().getHostAddress();
    }

    public int getHostPort() {
        if (socket == null) {
            return -1;
        }

        return socket.getPort();
    }

    public String getLclIp() {
        if (socket == null) {
            return null;
        }

        return socket.getLocalAddress().getHostAddress();
    }

    public int getLclPort() {
        if (socket == null) {
            return -1;
        }

        return socket.getLocalPort();
    }
}
