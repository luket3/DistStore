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
      ClientImp client = new ClientImp();
      client.addNodes();
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

      while(true) {
         String query = br.readLine();

         if (!client.sendQuery(query)) {
            System.out.println("Kill msg or invalid");
            continue;
         }
         String response = client.getResponse();
         System.out.println(response);
      }
   }
}
