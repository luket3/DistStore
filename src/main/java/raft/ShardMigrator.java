package raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import cluster.Node;
import communication.HandOff;
import message.MigrateData;
import com.google.gson.Gson;

/**
 * Delivers one shard data hand-over in the background.
 *
 * The {@link MigrateData} message is sent to a node of the target shard
 * (followers forward it to their leader). Sending is repeated, moving through
 * the shard's nodes, until the target leader acknowledges that the data is
 * committed or the attempts run out. The receiving shard only adds keys it
 * does not already have, so repeated deliveries are harmless.
 */
public class ShardMigrator implements Runnable {
    /** Delay between delivery attempts. */
    private static final int RETRY_INTERVAL_MS = 2000;

    /** Number of delivery attempts before giving up. */
    private static final int MAX_ATTEMPTS = 30;

    /** Hand-over to deliver. */
    private final MigrateData migration;

    /** Nodes of the target shard, in the order to try them. */
    private final List<Node> targets;

    /** Ids of hand-overs still waiting for an acknowledgement. */
    private final Set<String> pending;

    /** Log file for delivery progress. */
    private final String logPath;

    /** Serializer for the outgoing message. */
    private final Gson gson;

    /**
     * Creates a migrator for one hand-over.
     *
     * @param migration hand-over to deliver
     * @param targets nodes of the target shard, in the order to try them
     * @param pending ids of hand-overs still waiting for an acknowledgement;
     *     the id is removed from it when the acknowledgement arrives
     * @param logPath log file for delivery progress
     */
    public ShardMigrator(MigrateData migration, List<Node> targets, Set<String> pending, String logPath) {
        this.migration = migration;
        this.targets = new ArrayList<>(targets);
        this.pending = pending;
        this.logPath = logPath;
        this.gson = new Gson();
    }

    /**
     * Starts delivering a hand-over on a background thread.
     *
     * @param migration hand-over to deliver
     * @param targets nodes of the target shard, in the order to try them
     * @param pending ids of hand-overs still waiting for an acknowledgement
     * @param logPath log file for delivery progress
     */
    public static void start(MigrateData migration, List<Node> targets, Set<String> pending, String logPath) {
        pending.add(migration.id);
        Thread t = new Thread(new ShardMigrator(migration, targets, pending, logPath),
            "ShardMigrator-" + migration.shardId);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void run() {
        if (targets.isEmpty()) {
            HandOff.writeToFile("Migration " + migration.id + ": no nodes in shard " + migration.shardId + ", data dropped", logPath);
            pending.remove(migration.id);
            return;
        }

        String json = gson.toJson(migration);
        for (int attempt = 0; attempt < MAX_ATTEMPTS && pending.contains(migration.id); attempt++) {
            Node target = targets.get(attempt % targets.size());
            try {
                HandOff.sendToNode(target, json, logPath);
            } catch (Exception e) {
                HandOff.writeToFile("Migration " + migration.id + ": " + e.getMessage(), logPath);
            }

            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (pending.remove(migration.id)) {
            HandOff.writeToFile("Migration " + migration.id + " of " + migration.entries.size()
                + " keys to shard " + migration.shardId + " was never acknowledged", logPath);
        } else {
            HandOff.writeToFile("Migration " + migration.id + " of " + migration.entries.size()
                + " keys to shard " + migration.shardId + " acknowledged", logPath);
        }
    }

    /**
     * Orders a shard's nodes so nodes that were not part of the removed shard
     * are tried first. Those nodes were already in the target shard, so they
     * know its leader; nodes that just moved there may not yet.
     *
     * @param shardNodes nodes of the target shard
     * @param movedNodes nodes that belonged to the removed shard
     * @param selfId id of the local node, which is never targeted
     * @return nodes in the order to try them
     */
    public static List<Node> orderTargets(Map<String, Node> shardNodes, Map<String, Node> movedNodes, String selfId) {
        List<Node> established = new ArrayList<>();
        List<Node> moved = new ArrayList<>();
        for (Node n : shardNodes.values()) {
            if (n.id.equals(selfId))
                continue;
            if (movedNodes.containsKey(n.id))
                moved.add(n);
            else
                established.add(n);
        }
        established.addAll(moved);
        return established;
    }
}
