package runnable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import endpoints.ClientImp;
import message.Message;
import message.NodeMsg;

/**
 * Interactive CLI runner for the client-side endpoint.
 *
 * The class creates a ClientImp instance, initializes the local bootstrap
 * seed list, discovers the cluster view, and repeatedly accepts user
 * commands that are translated into protocol messages and dispatched to the
 * appropriate shard or spawner endpoint.
 */
public class Client {
    /**
     * Program entry point for the interactive client runner.
     *
     * The method creates the client endpoint, seeds the local node registry,
     * starts the response listener, fetches the cluster configuration, then
     * enters the console loop that builds, routes, and retries queries until a
     * valid response is received.
     *
     * @param args command-line arguments containing the client listen port
     * @throws Exception if client initialization or command processing fails
     */
   public static void main(String[] args) throws Exception {
      // Create the client endpoint and initialize the local seed list, then start
      // the listener thread that receives responses from the cluster.
      ClientImp client = new ClientImp(Integer.parseInt(args[0]));
      client.addSeeds();
      client.startListener();
      client.getCluster();
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

      // Enter the console loop that accepts user commands, builds the appropriate
      // protocol message, and dispatches it to the cluster until a valid response
      // is received.
      while(true) {
         // Read the next command from the console, build the corresponding message,
         // and send it to the cluster. If the response is invalid, retry until a
         // valid response is received.
         System.out.print("Enter command: ");
         String query = br.readLine();     
         String response = null;
         Message msg = client.buildMessage(query);
         if (msg == null) {
            System.out.println("Invalid input");
            continue;
         }
         
         // If the message is a NodeMsg, query the spawner to create/kill nodes 
         // as needed before sending the message to the cluster.
         if (msg.type.equals("NodeMsg")) {
            msg = client.querySpawner((NodeMsg) msg);
            if (msg == null) {
               System.out.println("Error querying spawner");
               continue;
            }
         }

         // Send the message to the cluster and retry if the response is 
         // invalid or indicates a configuration mismatch.
         while (true) {
            // Send the message to the cluster and wait for a response.
            client.sendQuery(msg);
            response = client.getResponse();
            // If the cluster doesn't respond, retry the query.
            if (response == null) {
               System.out.println("cluster didn't respond trying new node");
               continue;
            // If the cluster responds with an invalid configuration, fetch the new
            // configuration and retry the query.
            } else if (response.equals("Invalid config")) { //
               System.out.println("notified by client of invalid config");
               client.getCluster();
               msg = client.updateMessage(msg);
               continue;
            // If the cluster responds with an invalid response, retry the query.
            } else if (response.equals("Invalid response")) {
               System.out.println("Recieved Invalid Response");
            // If the cluster responds with a valid response, break out of the retry loop.
            } else
               break;
         }
         // Print the valid response from the cluster to the console.
         System.out.println("Cluster responded with: " + response);
      }
   }
}
