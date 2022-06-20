package kvraft

type RPCSessionArgs struct {
	SessionID int64
	Seq       int64
	Args      interface{}
}

const (
	RPCOK int = iota
	// The server has been killed
	RPCErrKilled
	// The server is not a leader. The client should try another server.
	RPCErrWrongLeader
	// There is a newer request with the same session ID
	RPCErrReplacedRequest
	// The request has been applied before but the reply has been removed from buffer.
	RPCErrObsoleteRequest
)

type RPCSessionReply struct {
	Err   int
	Reply interface{}
}

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
