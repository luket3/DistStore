package message;

/**
 * Sent by a shard leader once a {@link MigrateData} entry has been committed,
 * so the sender can stop retrying.
 */
public class MigrateAck extends Message {
    /** Id of the migration that was committed. */
    public String id;

    /**
     * Creates an acknowledgement for a committed migration.
     *
     * @param id id of the committed migration
     */
    public MigrateAck(String id) {
        super("MigrateAck", -1);
        this.id = id;
    }
}
