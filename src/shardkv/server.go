package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"

	sm "6.824/kvraft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrler_clerk *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int
	ss          sm.SessionServer
	// The last configuration in which all owned shards are received
	config_num_done int
	config          *shardctrler.Config
	// The configuration that should be applied after all owned shards in the
	// current configuration are received.
	config_pending []*shardctrler.Config
	// TODO: Only store the shards it serves
	shards                map[int]map[string]string
	num_shards_to_receive int

	mu sync.Mutex
	// The key is session ID
	shards_transmitting map[int64]*TransmittingShardsInfo

	session_id_top int64

	config_polled chan shardctrler.Config
	quit          chan struct{}
}

func (kv *ShardKV) Get(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	// Your code here.
	*reply = kv.ss.HandleRPC(rpc_args, raftStart, kv, true)
}

func (kv *ShardKV) PutAppend(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	// Your code here.
	*reply = kv.ss.HandleRPC(rpc_args, raftStart, kv, true)
}

// This was intended for the shard controller to push the new configurations to
// the shard servers. But the shard controller does not have make_end.
func (kv *ShardKV) Reconfigure(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	*reply = kv.ss.HandleRPC(rpc_args, raftStart, kv, false)
}

type Shard struct {
	ID int
	KV map[string]string
}

type ReceiveShardArgs struct {
	ConfigNum int
	Shards    []Shard
}

// Internal RPC
func (kv *ShardKV) ReceiveShards(rpc_args *sm.RPCSessionArgs, reply *sm.RPCSessionReply) {
	*reply = kv.ss.HandleRPC(rpc_args, raftStart, kv, false)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	// SessionServer shares the quit channel with ShardKV
	close(kv.quit)
	DPrintf("(%d,%d): Killed\n", kv.gid, kv.me)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	DPrintf("(%d,%d): Starting, maxraftstate = %d\n", gid, me, maxraftstate)

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(sm.RPCSessionArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ReceiveShardArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ctrler_clerk = shardctrler.MakeClerk(ctrlers)

	kv.quit = make(chan struct{})
	sm.InitSessionServer(&kv.ss, kv.quit)
	kv.initSomeState()

	snapshot := persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) == 0 {
		kv.lastApplied = kv.rf.LogBaseIndex
		kv.shards_transmitting = make(map[int64]*TransmittingShardsInfo)
		// TODO: A session id allocator
		kv.session_id_top = int64(kv.gid) * 100000000
	} else {
		kv.installSnapshot(kv.rf.LogBaseIndex, snapshot)
	}
	go kv.pollConfig(servers, kv.config.Num)
	go kv.Run()
	return kv
}

// Used internally
type TransmittingShardsInfo struct {
	SessionID int64
	Names     []string
	Args      *ReceiveShardArgs
}

func names2clients(names []string, make_end func(string) *labrpc.ClientEnd) []*labrpc.ClientEnd {
	clients := make([]*labrpc.ClientEnd, 0, len(names))
	for _, name := range names {
		clients = append(clients, make_end(name))
	}
	return clients
}

func (kv *ShardKV) pollConfig(servers []*labrpc.ClientEnd, num int) {
	var sc sm.SessionClient
	sm.InitSessionClient(&sc)

	t := time.NewTicker(100 * time.Millisecond)
	for {
		config := kv.ctrler_clerk.Query(num + 1)
		if config.Num < num {
			log.Panicf("Configuration number goes backward: %d -> %d\n",
				num, config.Num)
		}
		if config.Num == num {
			continue
		}
		num = config.Num
		// DPrintf("(%d,%d): New configuration: %v\n", kv.gid, kv.me, config)
		rpc_reply := sc.PutCommand(
			servers,
			"ShardKV.Reconfigure",
			config,
		)
		if rpc_reply.Err == sm.RPCErrKilled {
			return
		}
		if rpc_reply.Err != sm.RPCOK {
			log.Fatalf("(%d,%d): Reconfigure (%v) returns unexpected RPC error: %d\n", kv.gid, kv.me, config, rpc_reply.Err)
		}
		reply := rpc_reply.Reply
		if reply != nil {
			log.Fatalf("(%d,%d): Reconfigure (%v) reply non-nil value: %v\n", kv.gid, kv.me, config, reply)
		}
		// DPrintf("(%d,%d): Reconfigure (%v) done\n", kv.gid, kv.me, config)
		select {
		case <-kv.quit:
			t.Stop()
			return
		case <-t.C:
		}
		// DPrintf("(%d,%d): pollConfig continues\n", kv.gid, kv.me)
	}
}

func raftStart(c interface{}, rpc_args *sm.RPCSessionArgs, ss *sm.SessionServer) int {
	kv := c.(*ShardKV)
	// TODO: Search in the log first to avoid appending the same request multiple times to the Raft log.
	_, _, isLeader := kv.rf.Start(*rpc_args)
	if !isLeader {
		return sm.RPCErrWrongLeader
	}
	return sm.RPCOK
}

func execGet(c interface{}, a interface{}) interface{} {
	kv := c.(*ShardKV)
	args := a.(GetArgs)
	shard := key2shard(args.Key)
	shard_kv, ok := kv.shards[shard]
	if !ok {
		if kv.config.Shards[shard] != kv.gid {
			return GetReply{
				Err: ErrWrongGroup,
			}
		} else {
			return GetReply{
				Err: ErrUnderMigration,
			}
		}
	}
	if v, ok := shard_kv[args.Key]; ok {
		return GetReply{
			Err:   OK,
			Value: v,
		}
	} else {
		return GetReply{
			Err: ErrNoKey,
		}
	}
}

func execPutAppend(c interface{}, a interface{}) interface{} {
	kv := c.(*ShardKV)
	args := a.(PutAppendArgs)
	shard := key2shard(args.Key)
	shard_kv, ok := kv.shards[shard]
	if !ok {
		if kv.config.Shards[shard] != kv.gid {
			return ErrWrongGroup
		} else {
			return ErrUnderMigration
		}
	}
	if args.Op == "Append" {
		if v, ok := shard_kv[args.Key]; ok {
			v += args.Value
			shard_kv[args.Key] = v
		} else {
			shard_kv[args.Key] = args.Value
		}
	} else if args.Op == "Put" {
		shard_kv[args.Key] = args.Value
	} else {
		log.Fatalf("Unknown operator: %s\n", args.Op)
	}
	return OK
}

func (kv *ShardKV) transmitShards(info *TransmittingShardsInfo) {
	var sc sm.SessionClient
	sm.InitSessionClientWithSessionID(&sc, info.SessionID)
	names := info.Names
	args := info.Args

	DPrintf("(%d,%d): Session %d: Transmitting to %v: %v\n",
		kv.gid, kv.me, info.SessionID, names, args)
	target_group_peers := names2clients(names, kv.make_end)
	rpc_reply := sc.PutCommand(target_group_peers, "ShardKV.ReceiveShards", args)
	if rpc_reply.Err == sm.RPCErrKilled {
		fmt.Printf("Warning: %d: transmitShards(%v, %v) returns RPCErrKilled. Assuming transmitted successfully\n", sc.SessionID, args, names)
	} else {
		if rpc_reply.Err != sm.RPCOK {
			log.Fatalf("%d: transmitShards(%v, %v) returns unexpected RPC error: %d\n", sc.SessionID, args, names, rpc_reply.Err)
		}
		reply := rpc_reply.Reply
		if reply != nil {
			log.Fatalf("%d: transmitShards(%v, %v) replies non-nil value: %v\n",
				sc.SessionID, args, names, reply)
		}
	}
	kv.mu.Lock()
	delete(kv.shards_transmitting, info.SessionID)
	kv.mu.Unlock()
}

func (kv *ShardKV) newSessionID() int64 {
	kv.session_id_top += 1
	return kv.session_id_top
}

func (kv *ShardKV) checkNumShardsToReceive() {
	if kv.num_shards_to_receive == 0 {
		DPrintf("(%d,%d): Configuration done: %v\n", kv.gid, kv.me, kv.config)
		kv.config_num_done = kv.config.Num
		if len(kv.config_pending) != 0 {
			config := kv.config_pending[0]
			kv.config_pending = kv.config_pending[1:]
			if config.Num != kv.config.Num+1 {
				log.Panicf("(%d,%d): Not continuous configuration: %d. Previous configuration is %d\n",
					kv.gid, kv.me, config.Num, kv.config.Num)
			}
			kv.switchConfig(config)
		}
	}
}

func (kv *ShardKV) findShardsToReceive() {
	kv.num_shards_to_receive = 0
	for shard, gid := range kv.config.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := kv.shards[shard]; !ok {
			kv.num_shards_to_receive += 1
		}
	}
	// TODO: Use iteration instead of calling switchConfig recursively to
	// avoid stack overflow
	kv.checkNumShardsToReceive()
}

type GIDShards struct {
	gid    int
	shards []int
}

type GIDShardsSorter []GIDShards

func (a GIDShardsSorter) Len() int {
	return len(a)
}

func (a GIDShardsSorter) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a GIDShardsSorter) Less(i int, j int) bool {
	return a[i].gid < a[j].gid
}

func (kv *ShardKV) switchConfig(new_config *shardctrler.Config) {
	if new_config.Num <= kv.config.Num {
		log.Panicf("new_config.Num = %d, kv.config.Num = %d\n",
			new_config.Num, kv.config.Num)
	}
	if kv.config_num_done != kv.config.Num || kv.num_shards_to_receive != 0 {
		log.Panicf("The old configuration is still not done\n")
	}
	DPrintf("(%d,%d): Switching to configuration %v\n", kv.gid, kv.me, new_config)
	kv.config = new_config
	shards_to_migrate := make(map[int][]int) // gid -> shards
	for shard := range kv.shards {
		new_gid := new_config.Shards[shard]
		if new_gid != kv.gid {
			shards_to_migrate[new_gid] =
				append(shards_to_migrate[new_gid], shard)
		}
	}
	shards_to_migrate_array := make(GIDShardsSorter, 0, len(shards_to_migrate))
	for gid, shard_ids := range shards_to_migrate {
		shards_to_migrate_array = append(shards_to_migrate_array, GIDShards{
			gid:    gid,
			shards: shard_ids,
		})
	}
	sort.Sort(shards_to_migrate_array)
	// The set of shard IDs is deterministic.

	for _, gid_shards := range shards_to_migrate_array {
		gid := gid_shards.gid
		shard_ids := gid_shards.shards
		// The set of shard_ids is deterministic.
		shards := make([]Shard, 0, len(shard_ids))
		for _, id := range shard_ids {
			shard_kv := kv.shards[id]
			delete(kv.shards, id)
			shards = append(shards, Shard{ID: id, KV: shard_kv})
		}
		args := &ReceiveShardArgs{
			ConfigNum: new_config.Num,
			Shards:    shards,
		}
		names := new_config.Groups[gid]
		// Each session ID assigned to each TransmittingShardsInfo is
		// deterministic.
		session_id := kv.newSessionID()
		info := &TransmittingShardsInfo{session_id, names, args}
		kv.mu.Lock()
		kv.shards_transmitting[session_id] = info
		kv.mu.Unlock()
		go kv.transmitShards(info)
	}

	kv.findShardsToReceive()
}

// Generics require go 1.18
func copy_map(m map[string]string) map[string]string {
	ret := make(map[string]string)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

func copy_groups(m map[int][]string) map[int][]string {
	ret := make(map[int][]string)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

func copy_config(config *shardctrler.Config) *shardctrler.Config {
	return &shardctrler.Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: copy_groups(config.Groups),
	}
}

// TODO: Delta transmission of configuration
func execReconfigure(c interface{}, a interface{}) interface{} {
	kv := c.(*ShardKV)
	new_config := a.(shardctrler.Config)

	var latest_known_config *shardctrler.Config
	if len(kv.config_pending) != 0 {
		latest_known_config = kv.config_pending[len(kv.config_pending)-1]
	} else {
		latest_known_config = kv.config
	}
	if new_config.Num <= latest_known_config.Num {
		return nil
	}
	if new_config.Num > latest_known_config.Num+1 {
		log.Panicf("(%d,%d): Too new configuration %d, latest known is %d\n"+
			"kv.config: %v\nkv.config_pending: %v\n",
			kv.gid, kv.me, new_config.Num, latest_known_config.Num, kv.config,
			kv.config_pending)
	}
	DPrintf("(%d,%d): New known configuration: %v\n", kv.gid, kv.me, new_config)
	// fmt.Printf("kv.config: %v\n", kv.config)
	// fmt.Printf("kv.config_pending: %s\n", kv.sprintConfigPending())
	for shard, gid := range latest_known_config.Shards {
		if gid == 0 && new_config.Shards[shard] == kv.gid {
			kv.shards[shard] = make(map[string]string)
		}
	}
	if kv.config_num_done == kv.config.Num {
		if len(kv.config_pending) != 0 {
			log.Panicf("Expected nil pending configuration, found %v\n",
				kv.config_pending)
		}
		kv.switchConfig(copy_config(&new_config))
	} else {
		kv.config_pending = append(kv.config_pending, copy_config(&new_config))
	}
	return nil
}

func execReceiveShards(c interface{}, a interface{}) interface{} {
	kv := c.(*ShardKV)
	args := a.(ReceiveShardArgs)
	if args.ConfigNum <= kv.config_num_done {
		return nil
	}
	// If args.ConfigNum > kv.config.Num,
	// we still keeps the shard but does not serve it.
	// There will not be any problem since we will be the only owner.
	cnt_total := 0
	cnt_to_serve := 0
	for _, shard := range args.Shards {
		if _, ok := kv.shards[shard.ID]; !ok {
			kv.shards[shard.ID] = copy_map(shard.KV)
			cnt_total += 1
			if kv.config.Shards[shard.ID] == kv.gid {
				cnt_to_serve += 1
			}
		}
	}
	if cnt_total != 0 && cnt_total != len(args.Shards) {
		log.Panicf("(%d,%d): execReceiveShards: The received shards should be either all transmitted or all not transmitted: %v\n", kv.gid, kv.me, args)
	}
	DPrintf("(%d,%d): execReceiveShards done: %v\n", kv.gid, kv.me, args)
	if cnt_to_serve != 0 {
		kv.num_shards_to_receive -= cnt_to_serve
		kv.checkNumShardsToReceive()
	}
	return nil
}

func (kv *ShardKV) handleAppliedCommand(msg *raft.ApplyMsg) {
	ss := &kv.ss
	rpc_args := msg.Command.(sm.RPCSessionArgs)
	t := reflect.TypeOf(rpc_args.Args)
	if t == reflect.TypeOf(GetArgs{}) {
		ss.HandleAppliedCommand(&rpc_args, kv, execGet, true)
	} else if t == reflect.TypeOf(PutAppendArgs{}) {
		ss.HandleAppliedCommand(&rpc_args, kv, execPutAppend, true)
	} else if t == reflect.TypeOf(shardctrler.Config{}) {
		ss.HandleAppliedCommand(&rpc_args, kv, execReconfigure, false)
	} else if t == reflect.TypeOf(ReceiveShardArgs{}) {
		ss.HandleAppliedCommand(&rpc_args, kv, execReceiveShards, false)
	} else {
		log.Fatalf("Unknown type of command: %s\n", t.Name())
	}
}

func crashIf(e error) {
	if e != nil {
		panic(e.Error())
	}
}

func (kv *ShardKV) sprintShardTransmitting() string {
	kv.mu.Lock()
	ret := "["
	for shard, info := range kv.shards_transmitting {
		ret += fmt.Sprintf("{%d,{%v,%v}},", shard, info.Names, *info.Args)
	}
	ret += "]"
	kv.mu.Unlock()
	return ret
}

func (kv *ShardKV) sprintConfigPending() string {
	ret := "["
	for _, config := range kv.config_pending {
		// ret += fmt.Sprintf("%v,", *config)
		ret += fmt.Sprintf("%d,", config.Num)
	}
	ret += "]"
	return ret
}

func (kv *ShardKV) sprintState() string {
	return fmt.Sprintf("config = %v, shards = %v, shards_transmitting = %s, "+
		"config_num_done = %d, config = %v, config_pending = %v\n",
		kv.config, kv.shards, kv.sprintShardTransmitting(), kv.config_num_done,
		kv.config, kv.sprintConfigPending())
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.ss.EncodeTo(e)
	err := e.Encode(kv.config)
	crashIf(err)
	err = e.Encode(kv.shards)
	crashIf(err)
	err = e.Encode(kv.config_num_done)
	crashIf(err)

	err = e.Encode(len(kv.config_pending))
	crashIf(err)
	for _, config := range kv.config_pending {
		err = e.Encode(*config)
		crashIf(err)
	}

	kv.mu.Lock()
	err = e.Encode(kv.shards_transmitting)
	kv.mu.Unlock()
	crashIf(err)

	err = e.Encode(kv.session_id_top)
	crashIf(err)

	// DPrintf("(%d,%d): Snapshot made: %s\n", kv.gid, kv.me, kv.sprintState())
	DPrintf("(%d,%d): Snapshot made\n", kv.gid, kv.me)

	return w.Bytes()
}

func (kv *ShardKV) initSomeState() {
	kv.config_pending = make([]*shardctrler.Config, 0)
	kv.config = &shardctrler.Config{}
	kv.shards = make(map[int]map[string]string)
}

func (kv *ShardKV) installSnapshot(index int, snapshot []byte) {
	if kv.lastApplied >= index {
		// The snapshot is not newer. Just ignore it.
		return
	}
	// Now the previous transmitting go routines won't touch shards_transmitting
	// anymore
	kv.initSomeState()

	kv.lastApplied = index
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kv.ss.DecodeFrom(d)
	err := d.Decode(kv.config)
	crashIf(err)
	err = d.Decode(&kv.shards)
	crashIf(err)

	var config_num_done int
	err = d.Decode(&config_num_done)
	crashIf(err)
	kv.config_num_done = config_num_done

	var len int
	err = d.Decode(&len)
	crashIf(err)
	for len != 0 {
		config := new(shardctrler.Config)
		err = d.Decode(config)
		crashIf(err)
		kv.config_pending = append(kv.config_pending, config)
		len -= 1
	}

	kv.mu.Lock()
	kv.shards_transmitting = make(map[int64]*TransmittingShardsInfo)
	err = d.Decode(&kv.shards_transmitting)
	crashIf(err)
	for _, info := range kv.shards_transmitting {
		// Non-blocking
		go kv.transmitShards(info)
	}
	kv.mu.Unlock()

	var session_id_top int64
	err = d.Decode(&session_id_top)
	crashIf(err)
	kv.session_id_top = session_id_top

	kv.findShardsToReceive()
	DPrintf("(%d,%d): Snapshot installed: %s\n", kv.gid, kv.me, kv.sprintState())
}

func (kv *ShardKV) Run() {
	// To make sure that to make a new snapshot, at least one command has
	// been applied.
	last_applied_for_last_snapshot := 0
	for {
		select {
		case <-kv.quit:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if kv.lastApplied+1 != msg.CommandIndex {
					log.Fatalf("lastApplied = %d, applied command index = %d\n",
						kv.lastApplied, msg.CommandIndex)
				}
				kv.lastApplied = msg.CommandIndex
				kv.handleAppliedCommand(&msg)
			} else {
				if !msg.SnapshotValid {
					log.Fatalln("Applied message not command nor snapshot!")
				}
				kv.installSnapshot(msg.SnapshotIndex, msg.Snapshot)
				last_applied_for_last_snapshot = kv.lastApplied
			}
		case persistedSize := <-kv.rf.PersistedSizeCh:
			if kv.maxraftstate == -1 || persistedSize < kv.maxraftstate {
				break
			}
			DPrintf("(%d,%d): persistedSize = %d >= %d\n",
				kv.gid, kv.me, persistedSize, kv.maxraftstate)
			if last_applied_for_last_snapshot < kv.lastApplied {
				kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshot())
				last_applied_for_last_snapshot = kv.lastApplied
			} else {
				DPrintf("(%d,%d): Snapshot delayed\n", kv.gid, kv.me)
			}
		}
	}
}
