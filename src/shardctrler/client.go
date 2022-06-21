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
	sc sm.SessionClient
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	sm.InitSessionClient(&ck.sc, int64(len(servers)))
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	rpc_reply := ck.sc.PutCommand(queryRPC, ck, QueryArgs{num})
	if rpc_reply.Err == sm.RPCErrKilled {
		log.Fatalf("%d: Query (%d) returns RPCErrKilled\n", ck.sc.SessionID, num)
	}
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Query (%d) returns unexpected RPC error: %d\n", ck.sc.SessionID, num, rpc_reply.Err)
	}
	config := rpc_reply.Reply.(Config)
	DPrintf("%d: Query (%d) returns (%v)\n", ck.sc.SessionID, num, config)
	return config
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	servers_array := make([]GIDNames, 0, len(servers))
	for gid, names := range servers {
		servers_array = append(servers_array, GIDNames{gid, names})
	}
	rpc_reply := ck.sc.PutCommand(joinRPC, ck, JoinArgs{servers_array})
	if rpc_reply.Err == sm.RPCErrKilled {
		log.Fatalf("%d: Join (%v) returns RPCErrKilled\n", ck.sc.SessionID, servers)
	}
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Join (%v) returns unexpected RPC error: %d\n", ck.sc.SessionID, servers, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: Join (%v) reply non-nil value: %v\n", ck.sc.SessionID, servers, reply)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	rpc_reply := ck.sc.PutCommand(leaveRPC, ck, LeaveArgs{gids})
	if rpc_reply.Err == sm.RPCErrKilled {
		log.Fatalf("%d: Leave (%v) returns RPCErrKilled\n", ck.sc.SessionID, gids)
	}
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Leave (%v) returns unexpected RPC error: %d\n", ck.sc.SessionID, gids, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: Leave (%v) reply non-nil value: %v\n", ck.sc.SessionID, gids, reply)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	rpc_reply := ck.sc.PutCommand(moveRPC, ck, MoveArgs{shard, gid})
	if rpc_reply.Err == sm.RPCErrKilled {
		log.Fatalf("%d: Move (%d, %d) returns RPCErrKilled\n", ck.sc.SessionID, shard, gid)
	}
	if rpc_reply.Err != sm.RPCOK {
		log.Fatalf("%d: Move (%d, %d) returns unexpected RPC error: %d\n", ck.sc.SessionID, shard, gid, rpc_reply.Err)
	}
	reply := rpc_reply.Reply
	if reply != nil {
		log.Fatalf("%d: Move (%d, %d) reply non-nil value: %v\n", ck.sc.SessionID, shard, gid, reply)
	}
}

func queryRPC(c interface{}, i int64, args *sm.RPCSessionArgs) sm.RPCReply {
	ck := c.(*Clerk)
	var reply sm.RPCSessionReply
	ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply)
	return sm.RPCReply{Ok: ok, Reply: reply}
}

func joinRPC(c interface{}, i int64, args *sm.RPCSessionArgs) sm.RPCReply {
	ck := c.(*Clerk)
	var reply sm.RPCSessionReply
	ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply)
	return sm.RPCReply{Ok: ok, Reply: reply}
}

func leaveRPC(c interface{}, i int64, args *sm.RPCSessionArgs) sm.RPCReply {
	ck := c.(*Clerk)
	var reply sm.RPCSessionReply
	ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
	return sm.RPCReply{Ok: ok, Reply: reply}
}

func moveRPC(c interface{}, i int64, args *sm.RPCSessionArgs) sm.RPCReply {
	ck := c.(*Clerk)
	var reply sm.RPCSessionReply
	ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply)
	return sm.RPCReply{Ok: ok, Reply: reply}
}
