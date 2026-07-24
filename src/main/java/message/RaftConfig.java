package message;

import cluster.Node;
import java.util.Map;

/**
 * Carries a cluster-node configuration through the Raft message pipeline.
 */
public class RaftConfig extends Message {
	/** Nodes that make up the configuration being replicated. */
	public Map<String, Node> nodes;

	/**
	 * Creates an empty message for JSON deserialization.
	 */
	public RaftConfig() {
		super("RaftConfig", -1);
		this.nodes = null;
	}

	/**
	 * Creates a Raft configuration message.
	 *
	 * @param nodes node configuration to replicate
	 * @param version cluster configuration version
	 */
	public RaftConfig(Map<String, Node> nodes, int version) {
		super("RaftConfig", version);
		this.nodes = nodes;
	}
}
