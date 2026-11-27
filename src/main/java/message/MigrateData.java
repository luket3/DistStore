package message;

import java.util.HashMap;
import java.util.Map;
import cluster.Node;

/**
 * Hands key/value pairs from a shard that has been removed to the shard that
 * now owns them.
 *
 * The receiving shard replicates it through its Raft log like a client write.
 * Each pair is only stored if the key is not already present, so a newer
 * value written to the new owner is never overwritten and duplicate
 * deliveries from retries are harmless. Once committed, the receiving leader
 * answers {@link #client} with a {@link MigrateAck} carrying {@link #id}.
 *
 * The version is fixed at -1 so the message is never rejected as coming from
 * an old configuration and never makes a leader step down.
 */
public class MigrateData extends Reply {
    /** Unique id of this migration, echoed back in the acknowledgement. */
    public String id;

    /** Shard the data is meant for. */
    public String shardId;

    /** Key/value pairs to hand over. */
    public Map<String, String> entries;

    /**
     * Creates a migration message.
     *
     * @param id unique id of this migration
     * @param shardId shard the data is meant for
     * @param entries key/value pairs to hand over
     * @param sender node to acknowledge once the data is committed
     */
    public MigrateData(String id, String shardId, Map<String, String> entries, Node sender) {
        super("MigrateData", -1, sender);
        this.id = id;
        this.shardId = shardId;
        this.entries = new HashMap<>(entries);
    }
}
