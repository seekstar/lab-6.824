package kvraft

const (
	OK = "OK"
	// There is a newer request with the same session ID and sequence number
	ErrReplacedRequest = "ErrReplacedRequest"
	// The request has been applied before but the reply has been remove from buffer.
	ErrObsoleteRequest = "ErrObsoleteRequest"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
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
	SessionID int64
	Seq       int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SessionID int64
	Seq       int64
}

type GetReply struct {
	Err   Err
	Value string
}

type Reply interface {
	err() Err
}

func (r GetReply) err() Err {
	return r.Err
}

func (r PutAppendReply) err() Err {
	return r.Err
}
