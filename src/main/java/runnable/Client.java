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

import message.Message;
import message.NodeMsg;

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
      ClientImp client = new ClientImp(Integer.parseInt(args[0]));

      client.addSeeds();
      client.startListener();
      client.getCluster();
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

      while(true) {
         String query = br.readLine();     
         String response = null;
         Message msg = client.buildMessage(query);
         
         if (msg == null) {
            System.out.println("Invalid input");
            continue;
         }
         
         if (msg.type.equals("NodeMsg")) {
            msg = client.querySpawner((NodeMsg) msg);
            if (msg == null) {
               System.out.println("Error querying spawner");
               continue;
            }
         }

         while (true) {
            client.sendQuery(msg);
            response = client.getResponse();
            if (response == null) {
               System.out.println("cluster didn't respond trying new node");
               continue;
            }
            if (response.equals("Invalid config")) {
               System.out.println("notified by client of invalid config");
               client.getCluster();
               msg = client.updateMessage(msg);
               continue;
            } else if (response.equals("Invalid response")) {
               System.out.println("Recieved Invalid Response");
            } else
               break;
         }

         System.out.println("Cluster responded with: " + response);
      }
   }
}
