package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	// Wait a while for it to be migrated to this server.
	// TODO: When migrating a shard, there are several stages:
	// Stage 1: The target group is receiving the group, and the source group
	// still serves the shard, but keeps a operation log.
	// Stage 2: The source group stops serving the shard, and transmits the
	// operation log to the target group.
	// Stage 3: The target group serves the shard.
	// It is useless for this lab, because there is no server serving the shard
	// in stage 2. But I think the stalling is necessary to avoid split brain.
	ErrUnderMigration = "ErrUnderMigration"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
