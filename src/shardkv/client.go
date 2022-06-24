package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	sm "6.824/kvraft"
	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	shardctrler *shardctrler.Clerk
	config      *shardctrler.Config
	make_end    func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	sc sm.SessionClient
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.shardctrler = shardctrler.MakeClerk(ctrlers)
	DPrintf("Clerk for ShardKV: SessionID for Shard controller: %d\n", ck.shardctrler.SessionClient.SessionID)
	ck.config = &shardctrler.Config{}
	ck.make_end = make_end
	// You'll have to add code here.
	sm.InitSessionClient(&ck.sc, nil)
	DPrintf("Clerk for ShardKV: SessionID for ShardKV server: %d\n", ck.sc.SessionID)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	DPrintf("%d: Get(%s,shard=%d)\n", ck.sc.SessionID, key, shard)
	for {
		gid := ck.config.Shards[shard]
		if gid != 0 {
			var names []string
			var ok bool
			names, ok = ck.config.Groups[gid]
			if !ok {
				log.Panicf("Inconsistent configuration: %v\n", ck.config)
			}
			clients := names2clients(names, ck.make_end)
			rpc_reply := ck.sc.PutCommand(clients, "ShardKV.Get", GetArgs{key})
			if rpc_reply.Err == sm.RPCOK {
				reply := rpc_reply.Reply.(GetReply)
				if reply.Err == OK || reply.Err == ErrNoKey {
					return reply.Value
				} else if reply.Err == ErrUnderMigration {
					// TODO: Is 100ms suitable here?
					time.Sleep(100 * time.Millisecond)
					continue
				} else if reply.Err != ErrWrongGroup {
					log.Panicf("%d: Get(%s) returns unexpected error: %s\n",
						ck.sc.SessionID, key, reply.Err)
				}
			} else if rpc_reply.Err != sm.RPCErrKilled {
				log.Panicf("%d: Get(%s) returns unexpected RPC error: %d\n",
					ck.sc.SessionID, key, rpc_reply.Err)
			}
			// Treat RPCErrKilled as ErrWrongGroup, because the current trying group
			// might be a stale group and has been killed.
		}
		// ask controler for the latest configuration.
		config := ck.shardctrler.Query(-1)
		if config.Num == ck.config.Num {
			// Wait a while for the server to catch up
			// TODO: Is 100ms suitable here?
			time.Sleep(100 * time.Millisecond)
		} else {
			ck.config = &config
		}
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	DPrintf("%d: (%s,%s,%s,shard=%d)\n", ck.sc.SessionID, op, key, value, shard)
	for {
		gid := ck.config.Shards[shard]
		if gid != 0 {
			var names []string
			var ok bool
			names, ok = ck.config.Groups[gid]
			if !ok {
				log.Panicf("Inconsistent configuration: %v\n", ck.config)
			}
			clients := names2clients(names, ck.make_end)
			rpc_reply := ck.sc.PutCommand(
				clients,
				"ShardKV.PutAppend",
				PutAppendArgs{Key: key, Value: value, Op: op},
			)
			if rpc_reply.Err == sm.RPCOK {
				reply := rpc_reply.Reply.(string)
				if reply == OK {
					return
				} else if reply == ErrUnderMigration {
					// TODO: Is 100ms suitable here?
					time.Sleep(100 * time.Millisecond)
					continue
				} else if reply != ErrWrongGroup {
					log.Panicf("%d: (%s,%s,%s) returns unexpected error: %s\n",
						ck.sc.SessionID, op, key, value, reply)
				}
			} else if rpc_reply.Err != sm.RPCErrKilled {
				log.Panicf("%d: (%s,%s,%s) returns unexpected RPC error: %d\n",
					ck.sc.SessionID, op, key, value, rpc_reply.Err)
			}
			// Treat RPCErrKilled as ErrWrongGroup, because the current trying group
			// might be a stale group and has been killed.
		}
		// ask controler for the latest configuration.
		config := ck.shardctrler.Query(-1)
		if config.Num == ck.config.Num {
			// Wait a while for the server to catch up
			// TODO: Is 100ms suitable here?
			time.Sleep(100 * time.Millisecond)
		} else {
			ck.config = &config
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
