package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.824/labrpc"
)

type SessionClient struct {
	// TODO: Lease for each session.
	// The server should reject the new sessions whose sequence number is not 1.
	// But in this way the clerk can not be sure whether the last operation is
	// successful or not.
	// Maybe the transactions are necessary to handle this case.
	// The transactions are also needed to handle the crash of clerk.
	SessionID   int64
	seqTop      int64
	knownLeader int64
	serverNum   int64
}

func InitSessionClient(sc *SessionClient, serverNum int64) {
	sc.SessionID = nrand()
	sc.seqTop = 0
	sc.knownLeader = 0
	sc.serverNum = serverNum
	DPrintf("New session: %d\n", sc.SessionID)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	sc SessionClient
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sc *SessionClient) NewSeq() int64 {
	sc.seqTop += 1
	return sc.seqTop
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	InitSessionClient(&ck.sc, int64(len(ck.servers)))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("%d: Get (%s)\n", ck.sc.SessionID, key)
	rpc_reply := ck.sc.PutCommand(GetRPC, ck, GetArgs{Key: key})
	if rpc_reply.Err == RPCErrKilled {
		log.Fatalf("%d: Get (%s) returns RPCErrKilled\n", ck.sc.SessionID, key)
	}
	if rpc_reply.Err != RPCOK {
		log.Fatalf("%d: Get (%s) returns unexpected RPC error: %d\n", ck.sc.SessionID, key, rpc_reply.Err)
	}
	reply := rpc_reply.Reply.(GetReply)
	if reply.Err == ErrNoKey {
		DPrintf("%d: Get (%s) returns %s\n", ck.sc.SessionID, key, reply.Err)
		return ""
	}
	if reply.Err != OK {
		log.Fatalf("%d: Unexpected error in reply: %s\n", ck.sc.SessionID, reply.Err)
	}
	DPrintf("%d: Get (%s) returns (%s)\n", ck.sc.SessionID, key, reply.Value)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("%d: %s (%s) (%s)\n", ck.sc.SessionID, op, key, value)
	rpc_reply := ck.sc.PutCommand(
		PutAppendRPC,
		ck,
		PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
		},
	)
	if rpc_reply.Err == RPCErrKilled {
		log.Fatalf("%d: %s (%s) (%s) returns RPCErrKilled\n", ck.sc.SessionID, op, key, value)
	}
	if rpc_reply.Err != RPCOK {
		log.Fatalf("%d: %s (%s) (%s) returns unexpected RPC error: %d\n", ck.sc.SessionID, op, key, value, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: %s (%s) (%s) reply non-nil value: %v\n", ck.sc.SessionID, op, key, value, reply)
	}
	DPrintf("%d: %s (%s) (%s) done\n", ck.sc.SessionID, op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

type PutCommandRes struct {
	from  int64
	reply RPCSessionReply
}

type RPCReply struct {
	Ok    bool
	Reply RPCSessionReply
}

func GetRPC(c interface{}, i int64, args *RPCSessionArgs) RPCReply {
	ck := c.(*Clerk)
	var reply RPCSessionReply
	DPrintf("%d: Calling %d KVServer.Get\n", ck.sc.SessionID, i)
	ok := ck.servers[i].Call("KVServer.Get", args, &reply)
	return RPCReply{
		ok,
		reply,
	}
}

func PutAppendRPC(c interface{}, i int64, args *RPCSessionArgs) RPCReply {
	ck := c.(*Clerk)
	var reply RPCSessionReply
	DPrintf("%d: Calling %d KVServer.PutAppend\n", ck.sc.SessionID, i)
	ok := ck.servers[i].Call("KVServer.PutAppend", args, &reply)
	return RPCReply{
		ok,
		reply,
	}
}

func CallRPC(rpc func(interface{}, int64, *RPCSessionArgs) RPCReply, c interface{}, i int64, args *RPCSessionArgs, resChan chan PutCommandRes, abort chan struct{}) {
	rpcreply := rpc(c, i, args)
	if !rpcreply.Ok {
		// Assume that the outer logic has already been trying another server
		return
	}
	reply := rpcreply.Reply
	if reply.Err == RPCErrReplacedRequest {
		// The outer logic has already been trying another server
		return
	}
	select {
	case <-abort:
	case resChan <- PutCommandRes{
		from:  i,
		reply: reply,
	}:
	}
}

func (sc *SessionClient) PutCommand(rpc func(interface{}, int64, *RPCSessionArgs) RPCReply, c interface{}, args interface{}) RPCSessionReply {
	seq := sc.NewSeq()
	rpc_args := RPCSessionArgs{
		SessionID: sc.SessionID,
		Seq:       seq,
		Args:      args,
	}
	resChan := make(chan PutCommandRes)
	abort := make(chan struct{})
	i := sc.knownLeader
	for {
		go CallRPC(rpc, c, i, &rpc_args, resChan, abort)
		select {
		case <-time.After(time.Millisecond * 500):
			DPrintf("Timeout\n")
		case res := <-resChan:
			if res.reply.Err == RPCErrWrongLeader {
				break
			}
			if res.reply.Err == RPCErrObsoleteRequest {
				log.Fatalf("Unexpected RPC error: RPCErrObsoleteRequest\n")
			}
			// res.reply.Err still might be RPCErrKilled and RPCOK
			close(abort)
			sc.knownLeader = res.from
			return res.reply
		}
		i = (i + 1) % sc.serverNum
	}
}
