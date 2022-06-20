package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.824/labrpc"
)

type SessionClient struct {
	sessionID   int64
	seqTop      int64
	knownLeader int64
	serverNum   int64
}

func InitSessionClient(sc *SessionClient, serverNum int64) {
	sc.sessionID = nrand()
	sc.seqTop = 0
	sc.knownLeader = 0
	sc.serverNum = serverNum
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
	DPrintf("%d: Get (%s)\n", ck.sc.sessionID, key)
	reply := ck.sc.PutCommand(GetRPC, ck, GetArgs{Key: key}).(GetReply)
	DPrintf("%d: Get (%s) returns %s\n", ck.sc.sessionID, key, reply.err())
	if reply.err() == ErrNoKey {
		return ""
	}
	if reply.err() != OK {
		log.Fatalf("%d: Unexpected error in reply: %s\n", ck.sc.sessionID, reply.err())
	}
	DPrintf("%d: Get (%s) returns (%s)\n", ck.sc.sessionID, key, reply.Value)
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
	reply := ck.sc.PutCommand(
		PutAppendRPC,
		ck,
		PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
		},
	).(PutAppendReply)
	if reply.err() != OK {
		log.Fatalf("%d: Unexpected error in reply: %s\n", ck.sc.sessionID, reply.err())
	}
	DPrintf("%d: %s (%s) (%s) done\n", ck.sc.sessionID, op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("%d: Put (%s) (%s)\n", ck.sc.sessionID, key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("%d: Append (%s) (%s)\n", ck.sc.sessionID, key, value)
	ck.PutAppend(key, value, "Append")
}

type PutCommandRes struct {
	from  int64
	reply Reply
}

type RPCReply struct {
	ok    bool
	reply Reply
}

func GetRPC(c interface{}, i int64, args *RPCSessionArgs) RPCReply {
	ck := c.(*Clerk)
	var reply GetReply
	DPrintf("%d: Calling %d KVServer.Get\n", ck.sc.sessionID, i)
	ok := ck.servers[i].Call("KVServer.Get", args, &reply)
	return RPCReply{
		ok,
		reply,
	}
}

func PutAppendRPC(c interface{}, i int64, args *RPCSessionArgs) RPCReply {
	ck := c.(*Clerk)
	var reply PutAppendReply
	DPrintf("%d: Calling %d KVServer.PutAppend\n", ck.sc.sessionID, i)
	ok := ck.servers[i].Call("KVServer.PutAppend", args, &reply)
	return RPCReply{
		ok,
		reply,
	}
}

func CallRPC(rpc func(interface{}, int64, *RPCSessionArgs) RPCReply, c interface{}, i int64, args *RPCSessionArgs, resChan chan PutCommandRes, abort chan struct{}) {
	rpcreply := rpc(c, i, args)
	if !rpcreply.ok {
		// Assume that the outer logic has already been trying another server
		return
	}
	reply := rpcreply.reply
	if reply.err() == ErrReplacedRequest {
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

func (sc *SessionClient) PutCommand(rpc func(interface{}, int64, *RPCSessionArgs) RPCReply, c interface{}, args interface{}) Reply {
	seq := sc.NewSeq()
	rpc_args := RPCSessionArgs{
		SessionID: sc.sessionID,
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
			if res.reply.err() == "ErrWrongLeader" {
				break
			}
			close(abort)
			sc.knownLeader = res.from
			return res.reply
		}
		i = (i + 1) % sc.serverNum
	}
}
