package runnable;

/*
 * File: StartNodes.java
 * Project: Distributed KV Store
 * Author: Luke
 * Date: 2026-05-22
 * Description: Helper to launch multiple server nodes by reading network.config
 *              and spawning threads to run each server instance.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StartNodes {

    /**
     * Reads the network configuration file and creates a command line for each node.
     *
     * @return List of command strings, each suitable for launching a server instance.
     *         Each command has the form: "java Server <nodeId> <port>"
     */
    public static List<String> createCommands() throws IOException {
        List<String> commands = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("network.config"))) {
            String line;

            while ((line = br.readLine()) != null) {
                // CSV: nodeId,ip,port
                String[] parts = line.split(",");

                if (parts.length < 3) continue;

                String nodeId = parts[0].trim();
                String port   = parts[2].trim();

                String command = "java -cp target/classes endpoints.Server " + nodeId + " " + port;
                commands.add(command);
            }
        }

        return commands;
    }

    /**
     * Executes a given command using ProcessBuilder.
     *
     * @param command The command string to execute.
     */
    public static void runCommand(String command) {
        try {
            // Split command into tokens for ProcessBuilder
            String[] tokens = command.split(" ");

            ProcessBuilder pb = new ProcessBuilder(tokens);
            pb.inheritIO(); // Show output in this terminal
            pb.start().waitFor();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        List<String> commands = createCommands();
        List<Thread> threads = new ArrayList<>();

        // Launch a thread for each command
        for (String cmd : commands) {
            Thread t = new Thread(() -> runCommand(cmd));
            threads.add(t);
            t.start();
        }

        // Wait for all threads to finish
        for (Thread t : threads) {
            t.join();
        }
    }
}