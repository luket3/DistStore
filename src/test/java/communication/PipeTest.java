package communication;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import message.Ack;
import message.Message;
import message.Response;

class PipeTest {

    @Test
    void deliversInFifoOrder() throws Exception {
        Pipe pipe = new Pipe();
        Message a = new Response("a"), b = new Response("b");
        pipe.put(a);
        pipe.put(b);
        assertEquals(2, pipe.size());
        assertSame(a, pipe.take());
        assertSame(b, pipe.take());
        assertEquals(0, pipe.size());
    }

    @Test
    void timedTakeReturnsNullWhenEmpty() throws Exception {
        Pipe pipe = new Pipe();
        long start = System.currentTimeMillis();
        assertNull(pipe.take(100));
        assertTrue(System.currentTimeMillis() - start >= 90);
    }

    @Test
    void blockingTakeWakesWhenAnotherThreadPuts() throws Exception {
        Pipe pipe = new Pipe();
        Ack ack = new Ack();
        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            pipe.put(ack);
        });
        producer.start();
        assertSame(ack, pipe.take(2000));
        producer.join();
    }

    @Test
    void interruptedTakeThrows() throws Exception {
        Pipe pipe = new Pipe();
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, pipe::take);
    }
}
