package communication;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Simple blocking request handler that wraps a {@link ServerSocket}.
 */
public class Listener {
    /** Underlying TCP server socket used for listening. */
    private ServerSocket serverSocket;

    /**
     * Create and bind a listening socket on the given port.
     *
     * @param port local TCP port to listen on
     * @throws Exception on socket creation errors
     */
    public void createSocket(int port) throws Exception {
        serverSocket = new ServerSocket(port);
    }

    /**
     * Close the listening socket.
     *
     * @throws Exception on socket close errors
     */
    public void closeSocket() throws Exception {
        serverSocket.close();
    }

    /**
     * Block until an incoming connection is accepted and return the socket.
     *
     * @return accepted {@link Socket}
     * @throws Exception on accept failures
     */
    public Socket listenForConnection() throws Exception {
        Socket socket = serverSocket.accept();
        return socket;
    }

    /**
     * Listen for an incoming connection with a specified timeout.
     * Returns null if timeout occurs, otherwise returns the accepted socket.
     *
     * @param timeoutMillis timeout in milliseconds
     * @return accepted Socket or null if timeout
     * @throws Exception on accept failures (other than timeout)
     */
    public Socket listenForConnectionWithTimeout(
        int timeoutMillis
    ) throws Exception {
        // Set timeout on the ServerSocket
        serverSocket.setSoTimeout(timeoutMillis);
        try {
            Socket socket = serverSocket.accept();
            // Reset timeout to 0 (no timeout) for subsequent calls if needed
            serverSocket.setSoTimeout(0);
            return socket;
        } catch (SocketTimeoutException e) {
            // Timeout occurred
            return null;
        } finally {
            // Reset to no timeout for normal blocking calls
            serverSocket.setSoTimeout(0);
        }
    }
}