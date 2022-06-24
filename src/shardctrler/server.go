package shardctrler

import (
	"fmt"
	"log"
	"reflect"
	"sort"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	sm "6.824/kvraft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	ss          sm.SessionServer
	lastApplied int
	quit        chan struct{}
}

func raftStart(c interface{}, rpc_args *sm.RPCSessionArgs, ss *sm.SessionServer) int {
	sc := c.(*ShardCtrler)
	// TODO: Search in the log first to avoid appending the same request multiple times to the Raft log.
	_, _, isLeader := sc.rf.Start(*rpc_args)
	if !isLeader {
		return sm.RPCErrWrongLeader
	}
	return sm.RPCOK
}

func (sc *ShardCtrler) Join(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	// Your code here.
	*reply = sc.ss.HandleRPC(rpc_args, raftStart, sc, false)
}

func (sc *ShardCtrler) Leave(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	// Your code here.
	*reply = sc.ss.HandleRPC(rpc_args, raftStart, sc, false)
}

func (sc *ShardCtrler) Move(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	// Your code here.
	*reply = sc.ss.HandleRPC(rpc_args, raftStart, sc, false)
}

func (sc *ShardCtrler) Query(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	// Your code here.
	DPrintf("Shard Controller %d: Query RPC: %v\n", sc.me, rpc_args)
	*reply = sc.ss.HandleRPC(rpc_args, raftStart, sc, true)
	DPrintf("Shard Controller %d: Query RPC: %v, reply: %v\n", sc.me, rpc_args, *reply)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.quit)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(sm.RPCSessionArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(Config{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = sc.rf.LogBaseIndex
	sc.quit = make(chan struct{})
	sm.InitSessionServer(&sc.ss, sc.quit)

	go sc.Run()

	return sc
}

// Generics require go 1.18
func copy_map(m map[int][]string) map[int][]string {
	ret := make(map[int][]string)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

func exeJoin(c interface{}, a interface{}) interface{} {
	sc := c.(*ShardCtrler)
	args := a.(JoinArgs)
	old_config := &sc.configs[len(sc.configs)-1]
	sc.configs = append(sc.configs, Config{
		Num:    old_config.Num + 1,
		Shards: old_config.Shards,
		Groups: copy_map(old_config.Groups),
	})
	new_config := &sc.configs[len(sc.configs)-1]

	old_group_num := len(old_config.Groups)
	new_group_num := old_group_num + len(args.Servers)
	new_min_shard_per_group := NShards / new_group_num
	allowed_min_plus_1 := NShards % new_group_num

	new_server_gid := make([]int, 0, len(args.Servers))
	// Deterministic iteration
	for _, gidNames := range args.Servers {
		new_config.Groups[gidNames.GID] = gidNames.Names
		new_server_gid = append(new_server_gid, gidNames.GID)
	}

	var allowed_min_plus_1_in_new_servers int
	if allowed_min_plus_1 <= old_group_num {
		allowed_min_plus_1_in_new_servers = 0
	} else {
		allowed_min_plus_1_in_new_servers = allowed_min_plus_1 - old_group_num
		allowed_min_plus_1 = old_group_num
	}

	cur := 0
	filled := 0
	fill_new_server := func(shard int) {
		if filled >= new_min_shard_per_group {
			if filled == new_min_shard_per_group && allowed_min_plus_1_in_new_servers != 0 {
				allowed_min_plus_1_in_new_servers -= 1
			} else {
				cur += 1
				filled = 0
			}
		}
		new_config.Shards[shard] = new_server_gid[cur]
		filled += 1
	}
	gid_shard_num := make(map[int]int)
	// Deterministic iteration
	for shard, gid := range old_config.Shards {
		if gid == 0 {
			fill_new_server(shard)
		} else if gid_shard_num[gid] >= new_min_shard_per_group {
			if gid_shard_num[gid] == new_min_shard_per_group && allowed_min_plus_1 > 0 {
				allowed_min_plus_1 -= 1
				gid_shard_num[gid] += 1
			} else {
				fill_new_server(shard)
			}
		} else {
			gid_shard_num[gid] += 1
		}
	}
	funcPanic := func() {
		panic(fmt.Sprintf("%d: exeJoin (%v): old_config = %v\n", sc.me, args.Servers, *old_config))
	}
	if filled < new_min_shard_per_group {
		fmt.Printf("filled = %d, new_min_shard_per_group = %d\n", filled, new_min_shard_per_group)
		funcPanic()
	}
	if allowed_min_plus_1_in_new_servers != 0 {
		fmt.Printf("allowed_min_plus_1_in_new_servers = %d\n", allowed_min_plus_1_in_new_servers)
		funcPanic()
	}
	cur += 1
	filled = 0
	if cur != len(new_server_gid) {
		if new_min_shard_per_group != 0 {
			panic(fmt.Sprintf("%d: exeJoin (%v): cur = %d, new_min_shard_per_group = %d\n", sc.me, args.Servers, cur, new_min_shard_per_group))
		}
		DPrintf("%d: exeJoin (%v): New server not assigned any shard: %v\n", sc.me, args.Servers, new_server_gid[cur:])
	}
	if allowed_min_plus_1 != 0 {
		fmt.Printf("allowed_min_plus_1 = %d\n", allowed_min_plus_1)
		funcPanic()
	}
	return nil
}

type shardNumGID struct {
	shard_num int
	gid       int
}

type shardNumGIDSorter []shardNumGID

func (a shardNumGIDSorter) Len() int {
	return len(a)
}

func (a shardNumGIDSorter) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a shardNumGIDSorter) Less(i int, j int) bool {
	if a[i].shard_num != a[j].shard_num {
		return a[i].shard_num > a[j].shard_num
	} else {
		return a[i].gid < a[j].gid
	}
}

func exeLeave(c interface{}, a interface{}) interface{} {
	sc := c.(*ShardCtrler)
	args := a.(LeaveArgs)
	old_config := &sc.configs[len(sc.configs)-1]

	old_group_num := len(old_config.Groups)
	new_group_num := old_group_num - len(args.GIDs)
	if new_group_num == 0 {
		DPrintf("%d: All groups leave.\n", sc.me)
		new_config := Config{Num: old_config.Num + 1}
		sc.configs = append(sc.configs, new_config)
		return nil
	}

	sc.configs = append(sc.configs, Config{
		Num:    old_config.Num + 1,
		Shards: old_config.Shards,
		Groups: copy_map(old_config.Groups),
	})
	new_config := &sc.configs[len(sc.configs)-1]

	// The set of elements in it is deterministic
	gid_to_leave := make(map[int]struct{})
	for _, gid := range args.GIDs {
		gid_to_leave[gid] = struct{}{}
		delete(new_config.Groups, gid)
	}

	// The set of elements in it is deterministic
	gid_shard_num := make(map[int]int)
	for gid := range old_config.Groups {
		if _, ok := gid_to_leave[gid]; !ok {
			gid_shard_num[gid] = 0
		}
	}
	shards_to_move := make([]int, 0)
	// Deterministic iteration
	for shard, gid := range old_config.Shards {
		// Deterministic branch
		if _, ok := gid_to_leave[gid]; ok {
			shards_to_move = append(shards_to_move, shard)
		} else {
			gid_shard_num[gid] += 1
		}
	}
	// shards_to_move is deterministic

	shard_num_gid_array := make([]shardNumGID, 0, new_group_num)
	for gid, shard_num := range gid_shard_num {
		shard_num_gid_array = append(shard_num_gid_array, shardNumGID{shard_num, gid})
	}
	// The set of elements in shard_num_gid_array is deterministic
	sort.Sort(shardNumGIDSorter(shard_num_gid_array))
	// shard_num_gid_array becomes deterministic

	next_to_move := 0
	new_min_shard_per_group := NShards / new_group_num
	allowed_min_plus_1 := NShards % new_group_num
	for _, shard_num_gid := range shard_num_gid_array {
		shard_num := shard_num_gid.shard_num
		gid := shard_num_gid.gid
		var target int
		if allowed_min_plus_1 != 0 {
			target = new_min_shard_per_group + 1
			allowed_min_plus_1 -= 1
		} else {
			target = new_min_shard_per_group
		}
		if shard_num > target {
			log.Fatalf("shard_num = %d > target = %d\n", shard_num, target)
		}
		for shard_num < target {
			new_config.Shards[shards_to_move[next_to_move]] = gid
			next_to_move += 1
			shard_num += 1
		}
	}
	if next_to_move != len(shards_to_move) || allowed_min_plus_1 != 0 {
		panic(fmt.Sprintf("%d: next_to_move = %d, allowed_min_plus_1 = %d\n", sc.me, next_to_move, allowed_min_plus_1))
	}
	return nil
}

func exeMove(c interface{}, a interface{}) interface{} {
	sc := c.(*ShardCtrler)
	args := a.(MoveArgs)
	old_config := &sc.configs[len(sc.configs)-1]
	sc.configs = append(sc.configs, Config{
		Num:    old_config.Num + 1,
		Shards: old_config.Shards,
		Groups: copy_map(old_config.Groups),
	})
	new_config := &sc.configs[len(sc.configs)-1]
	new_config.Shards[args.Shard] = args.GID
	return nil
}

func exeQuery(c interface{}, a interface{}) interface{} {
	sc := c.(*ShardCtrler)
	args := a.(QueryArgs)
	if args.Num == -1 || args.Num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[args.Num]
	}
}

func (sc *ShardCtrler) handleAppliedCommand(msg *raft.ApplyMsg) {
	rpc_args := msg.Command.(sm.RPCSessionArgs)
	t := reflect.TypeOf(rpc_args.Args)
	if t == reflect.TypeOf(JoinArgs{}) {
		sc.ss.HandleAppliedCommand(&rpc_args, sc, exeJoin, false)
	} else if t == reflect.TypeOf(LeaveArgs{}) {
		sc.ss.HandleAppliedCommand(&rpc_args, sc, exeLeave, false)
	} else if t == reflect.TypeOf(MoveArgs{}) {
		sc.ss.HandleAppliedCommand(&rpc_args, sc, exeMove, false)
	} else if t == reflect.TypeOf(QueryArgs{}) {
		sc.ss.HandleAppliedCommand(&rpc_args, sc, exeQuery, true)
	} else {
		log.Fatalf("Unknown type of command: %s\n", t.Name())
	}
}

func (sc *ShardCtrler) Run() {
	for {
		select {
		case <-sc.quit:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				if sc.lastApplied+1 != msg.CommandIndex {
					log.Fatalf("lastApplied = %d, applied command index = %d\n", sc.lastApplied, msg.CommandIndex)
				}
				sc.lastApplied = msg.CommandIndex
				sc.handleAppliedCommand(&msg)
			} else {
				log.Fatalf("Applied message not command: %v\n", msg)
			}
		}
	}
}
