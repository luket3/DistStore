package communication;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import message.Message;
import message.Response;
import testutil.Capture;
import testutil.Nodes;

class HandOffTest {

    @Test
    void writeToFileCreatesDirectoriesAndAppendsTimestampedLines(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("a/b/test.log");
        HandOff.writeToFile("first", file.toString());
        HandOff.writeToFile("second", file.toString());

        List<String> lines = Files.readAllLines(file);
        assertEquals(2, lines.size());
        assertTrue(lines.get(0).endsWith(" first"));
        assertTrue(lines.get(1).endsWith(" second"));
        // ISO-8601 timestamp prefix
        assertTrue(lines.get(0).matches("^\\d{4}-\\d{2}-\\d{2}T.*Z first$"));
    }

    @Test
    void sendToNodeDeliversAndLogs(@TempDir Path dir) throws Exception {
        try (Capture capture = new Capture()) {
            Path log = dir.resolve("send.log");
            HandOff.sendToNode(capture.node("target"), Capture.GSON.toJson(new Response("hi")), log.toString());

            Message m = capture.next(2000);
            assertTrue(m instanceof Response);
            assertEquals("hi", ((Response) m).msg);
            assertTrue(Files.readString(log).contains("Sending message to node target"));
        }
    }

    @Test
    void sendToUnreachableNodeThrows(@TempDir Path dir) {
        RuntimeException e = assertThrows(RuntimeException.class,
            () -> HandOff.sendToNode(Nodes.unreachable("down"), "{}", dir.resolve("x.log").toString()));
        assertTrue(e.getMessage().contains("down"));
    }
}
