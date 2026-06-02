package communication;

/*
 * File: Pipe.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-23
 * Description: Thread-safe pipe for passing messages between components.
 */

import java.util.LinkedList;
import java.util.Queue;

/**
 * Thread-safe pipe for passing {@link MessageInfo} messages between threads.
 * Implements an unbounded buffer with blocking take operations and
 * timeout support.
 */
public class Pipe {
    private final Queue<MessageInfo> queue;

    /**
     * Creates an unbounded pipe.
     */
    public Pipe() {
        this.queue = new LinkedList<>();
    }

    /**
     * Puts a message into the pipe.
     * Never blocks due to capacity (unbounded).
     *
     * @param message the message to put into the pipe
     */
    public synchronized void put(String message) {
        queue.add(new MessageInfo(message));
        notifyAll(); // Notify threads waiting to take
    }

    public synchronized void put(MessageInfo message) {
        queue.add(message);
        notifyAll(); // Notify threads waiting to take
    }

    /**
    * Takes a message from the pipe, blocking if necessary until a message is
    * available or the specified timeout elapses.
     *
    * @param timeoutMillis the maximum time to wait in milliseconds, or 0 to
    *                      wait indefinitely
     * @return the message taken from the pipe, or null if timeout elapsed
    * @throws InterruptedException if the current thread is interrupted while
    *                              waiting
     */
    public synchronized MessageInfo takeAll(
        long timeoutMillis
    ) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long remainingTime = timeoutMillis;

        while (queue.isEmpty()) {
            if (timeoutMillis > 0) {
                if (remainingTime <= 0) {
                    return null; // Timeout elapsed
                }
                wait(remainingTime);
                long elapsed = System.currentTimeMillis() - startTime;
                remainingTime = timeoutMillis - elapsed;
            } else {
                // Wait indefinitely
                wait();
            }
        }

        MessageInfo msg = queue.poll();
        // Notify threads waiting to put (though not needed for unbounded)
        notifyAll();
        return msg;
    }

    public synchronized MessageInfo takeAll() throws InterruptedException {
        return takeAll(0); // 0 means wait indefinitely
    }

    public synchronized String take(
        long timeoutMillis
    ) throws InterruptedException {
        MessageInfo msg = takeAll(timeoutMillis);
        if (msg == null) {
            return null;
        }
        return msg.message;
    }

    /**
    * Takes a message from the pipe, blocking indefinitely until a message
    * is available.
     * This method is provided for backward compatibility.
     *
     * @return the message taken from the pipe
    * @throws InterruptedException if the current thread is interrupted while
    *                              waiting
     */
    public synchronized String take() throws InterruptedException {
        return take(0); // 0 means wait indefinitely
    }

    /**
     * Gets the current number of messages in the pipe.
     *
     * @return the number of messages currently in the pipe
     */
    public synchronized int size() {
        return queue.size();
    }
}