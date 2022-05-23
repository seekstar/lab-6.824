package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	sessionID   int64
	seqTop      int64
	knownLeader int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) NewSeq() int64 {
	ck.seqTop += 1
	return ck.seqTop
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.sessionID = nrand()
	ck.seqTop = 0
	ck.knownLeader = 0
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
	DPrintf("%d: Get (%s)\n", ck.sessionID, key)
	seq := ck.NewSeq()
	args := GetArgs{
		Key:       key,
		SessionID: ck.sessionID,
		Seq:       seq,
	}
	reply := ck.PutCommand(GetRPC, &args).(GetReply)
	if reply.err() == ErrNoKey {
		DPrintf("%d: Get (%s) returns ErrNoKey\n", ck.sessionID, key)
		return ""
	}
	if reply.err() != OK {
		log.Fatalf("%d: Unexpected error in reply: %s\n", ck.sessionID, reply.err())
	}
	DPrintf("%d: Get (%s) returns (%s)\n", ck.sessionID, key, reply.Value)
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
	seq := ck.NewSeq()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		SessionID: ck.sessionID,
		Seq:       seq,
	}
	reply := ck.PutCommand(PutAppendRPC, &args).(PutAppendReply)
	if reply.err() != OK {
		log.Fatalf("%d: Unexpected error in reply: %s\n", ck.sessionID, reply.err())
	}
	DPrintf("%d: %s (%s) (%s) done\n", ck.sessionID, op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("%d: Put (%s) (%s)\n", ck.sessionID, key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("%d: Append (%s) (%s)\n", ck.sessionID, key, value)
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

func GetRPC(ck *Clerk, i int64, args interface{}) RPCReply {
	var reply GetReply
	DPrintf("%d: Calling %d KVServer.Get\n", ck.sessionID, i)
	ok := ck.servers[i].Call("KVServer.Get", args, &reply)
	return RPCReply{
		ok,
		reply,
	}
}

func PutAppendRPC(ck *Clerk, i int64, args interface{}) RPCReply {
	var reply PutAppendReply
	DPrintf("%d: Calling %d KVServer.PutAppend\n", ck.sessionID, i)
	ok := ck.servers[i].Call("KVServer.PutAppend", args, &reply)
	return RPCReply{
		ok,
		reply,
	}
}

func (ck *Clerk) CallRPC(rpc func(*Clerk, int64, interface{}) RPCReply, i int64, args interface{}, resChan chan PutCommandRes, abort chan struct{}) {
	rpcreply := rpc(ck, i, args)
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

func (ck *Clerk) PutCommand(rpc func(*Clerk, int64, interface{}) RPCReply, args interface{}) Reply {
	resChan := make(chan PutCommandRes)
	abort := make(chan struct{})
	i := ck.knownLeader
	for {
		go ck.CallRPC(rpc, i, args, resChan, abort)
		select {
		case <-time.After(time.Millisecond * 500):
			DPrintf("Timeout\n")
		case res := <-resChan:
			if res.reply.err() == "ErrWrongLeader" {
				break
			}
			close(abort)
			ck.knownLeader = res.from
			return res.reply
		}
		i = (i + 1) % int64(len(ck.servers))
	}
}
