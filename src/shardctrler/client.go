package shardctrler

//
// Shardctrler clerk.
//

import (
	"log"

	// Session Manager
	sm "6.824/kvraft"
	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	SessionClient sm.SessionClient
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	sm.InitSessionClient(&ck.SessionClient, nil)
	return ck
}

func MakeClerkV2(servers []*labrpc.ClientEnd, quit chan struct{}) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	sm.InitSessionClient(&ck.SessionClient, quit)
	return ck
}

func (ck *Clerk) QueryV2(num int) *Config {
	// Your code here.
	rpc_reply := ck.SessionClient.PutCommand(ck.servers, "ShardCtrler.Query", QueryArgs{num})
	if rpc_reply.Err == sm.RPCErrAbort {
		return nil
	}
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Query (%d) returns unexpected RPC error: %d\n", ck.SessionClient.SessionID, num, rpc_reply.Err)
	}
	config := rpc_reply.Reply.(Config)
	DPrintf("%d: Query (%d) returns (%v)\n", ck.SessionClient.SessionID, num, config)
	return &config
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	return *ck.QueryV2(num)
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	servers_array := make([]GIDNames, 0, len(servers))
	for gid, names := range servers {
		servers_array = append(servers_array, GIDNames{gid, names})
	}
	rpc_reply := ck.SessionClient.PutCommand(ck.servers, "ShardCtrler.Join", JoinArgs{servers_array})
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Join (%v) returns unexpected RPC error: %d\n", ck.SessionClient.SessionID, servers, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: Join (%v) reply non-nil value: %v\n", ck.SessionClient.SessionID, servers, reply)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	rpc_reply := ck.SessionClient.PutCommand(ck.servers, "ShardCtrler.Leave", LeaveArgs{gids})
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Leave (%v) returns unexpected RPC error: %d\n", ck.SessionClient.SessionID, gids, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: Leave (%v) reply non-nil value: %v\n", ck.SessionClient.SessionID, gids, reply)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	rpc_reply := ck.SessionClient.PutCommand(ck.servers, "ShardCtrler.Move", MoveArgs{shard, gid})
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Move (%d, %d) returns unexpected RPC error: %d\n", ck.SessionClient.SessionID, shard, gid, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: Move (%d, %d) reply non-nil value: %v\n", ck.SessionClient.SessionID, shard, gid, reply)
	}
}
