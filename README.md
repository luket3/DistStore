# dist_KV_store
this is a personal project, making a distributed key value store


store shards in consistent hash table

create server side hash tables containing shards

one instance of raft operates acorss a shard

maintain shards in Consistent hash

use raft on shard groups to control adding/removing/spliting shards/nodes from network

add node to first shard clockwise from it on hash ring

clients discover nodes on network by requesting it from a seed node

# message protocol

## node operation

add <id> <ip> <port>

remove <id>

## dict operation

<'Get' | 'Delete'> <key>

Put <key> <value>


## Raft communication

The Raft consensus algorithm is used to manage replicated logs and ensure consistency across distributed nodes. This implementation uses Raft for two distinct purposes:

1. **Cluster-wide Raft**: Manages membership changes (adding/removing nodes/shards) across the entire cluster
2. **Shard-wide Raft**: Manages state machine operations (Get/Put/Delete) within individual shards

### Raft Message Types

The Raft protocol involves several types of RPC messages exchanged between nodes:

#### 1. Basic Raft RPCs (Internal to Raft Consensus)
These messages are used by the Raft algorithm itself to maintain consensus:

- **RequestVote**: Sent by candidates to gather votes during leader election
  - Fields: term, candidateId, lastLogIndex, lastLogTerm
  - Response: term, voteGranted

- **AppendEntries**: Sent by leaders to replicate log entries and provide heartbeat
  - Fields: term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit
  - Response: term, success, matchIndex (when successful)

#### 2. Application-Level Messages (Handled by Raft Log)
These are the actual operations that clients want to perform, which get replicated via Raft:

- **Cluster-wide Operations** (handled by cluster-wide Raft group):
  - `add <id> <ip> <port>` - Add a new node to the cluster
  - `remove <id>` - Remove a node from the cluster

- **Shard-wide Operations** (handled by shard-wide Raft group):
  - `Get <key>` - Retrieve a value from the key-value store
  - `Put <key> <value>` - Store a key-value pair
  - `Delete <key>` - Remove a key-value pair

#### 3. Internal Cluster Management Messages
These messages handle cluster configuration changes and inter-node communication:

- **JoinRequest**: Sent by new nodes to join the cluster
- **JoinResponse**: Sent by existing nodes to accept/reject join requests
- **LeaveNotification**: Sent by nodes intending to leave the cluster
- **ShardTransfer**: Sent when migrating shards between nodes during rebalancing
- **SnapshotTransfer**: Used for installing snapshots to speed up follower catch-up

### Message Flow

1. Client sends operation (e.g., `Put key value`) to any node
2. Receiving node forwards to appropriate shard's Raft group leader
3. Leader appends operation to its log and sends AppendEntries to followers
4. Once replicated to majority, leader applies operation to state machine
5. Leader returns result to client

For cluster membership changes:
1. Client sends `add` or `remove` command
2. Command goes to cluster-wide Raft group leader
3. Leader replicates configuration change
4. Once committed, cluster topology is updated
5. New/removed nodes begin/cease participating in both Raft groups

This separation of concerns allows the system to independently scale shard operations while maintaining coherent cluster membership.



make so in server it makes 2 threads, one for cluster wide raft, and one for shard wide raft, it then forwards requests to the correct raft type, it make each of these on a seperate port (maybe create class to communicate with raft), and also forwards requests to the leader if it arrived at the not leader

compile command:
javac -d out (Get-ChildItem -Recurse -Filter *.java | ForEach-Object { $_.FullName })