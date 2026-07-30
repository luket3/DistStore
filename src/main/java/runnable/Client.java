package runnable;

/*
 * File: Client_run_instance.java
 * Project: Distributed KV Store
 * Author: luket
 * Date: 2026-05-22
 * Description: Simple runner that creates a Client instance and initializes
 * the client-side view of the cluster.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import endpoints.ClientImp;

/**
 * Simple runner that creates a {@code Client} instance and initializes the
 * client-side view of the cluster.
 */
public class Client {
    /**
     * Program entry point. Initializes a {@code Client}, loads nodes from
     * configuration, and opens a console reader for interactive queries.
     *
     * @param args command-line arguments (unused)
     * @throws Exception if initialization fails
     */
   public static void main(String[] args) throws Exception {

      List<Thread> threads = new ArrayList<>();
      ClientImp client = new ClientImp(threads);

      client.addSeeds();
      client.startListener();
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

      while(true) {
         String query = br.readLine();

         if (query.equals("exit") || query.equals("Exit")) {
            break;
         }

         client.initCluster();
         String output = client.sendQuery(query);
         System.out.println(output);
         if (output.equals("Invalid query"))
            continue;

         try {
            String response = client.getResponse();
            System.out.println("client responded with: " + response);
         } catch (Exception e) {
            System.err.println("Error getting response: " + e);
         }
      }

      // Wait for all threads to finish
      for (Thread t : threads) {
         t.join();
      }
   }
}
