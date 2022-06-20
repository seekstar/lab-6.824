package kvraft

import (
	"bytes"
	"log"
	"reflect"
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type replyWithSeq struct {
	seq   int64
	reply RPCSessionReply
}

type replyChanWithSeq struct {
	seq       int64
	replyChan chan RPCSessionReply
}

type SessionServer struct {
	mu         sync.Mutex
	replyChans map[int64]replyChanWithSeq
	replyBuf   map[int64]replyWithSeq
	quit       chan struct{}
}

func (ss *SessionServer) checkBuf(rpc_args *RPCSessionArgs, reply *RPCSessionReply, withRet bool) bool {
	ss.mu.Lock()
	buf, ok := ss.replyBuf[rpc_args.SessionID]
	ss.mu.Unlock()
	if ok {
		if buf.seq == rpc_args.Seq {
			*reply = buf.reply
			return true
		} else if buf.seq > rpc_args.Seq {
			if withRet {
				reply.Err = RPCErrObsoleteRequest
			} else {
				reply.Err = RPCOK
			}
			return true
		}
	}
	return false
}

func (ss *SessionServer) HandleRPC(rpc_args *RPCSessionArgs, do func(interface{}, *RPCSessionArgs, *SessionServer), c interface{}, withRet bool) (reply RPCSessionReply) {
	if ss.checkBuf(rpc_args, &reply, withRet) {
		return
	}
	replyChan := make(chan RPCSessionReply, 1)
	ss.mu.Lock()
	if original, ok := ss.replyChans[rpc_args.SessionID]; ok {
		close(original.replyChan)
	}
	ss.replyChans[rpc_args.SessionID] = replyChanWithSeq{
		seq:       rpc_args.Seq,
		replyChan: replyChan,
	}
	ss.mu.Unlock()
	do(c, rpc_args, ss)
	select {
	case <-ss.quit:
		reply.Err = RPCErrKilled
	case r, ok := <-replyChan:
		if ok {
			reply = r
		} else {
			// The channel has been closed.
			if ss.checkBuf(rpc_args, &reply, withRet) {
				// It has been answered
			} else {
				// It has been replaced
				reply.Err = RPCErrReplacedRequest
			}
		}
	}
	ss.mu.Lock()
	if r, ok := ss.replyChans[rpc_args.SessionID]; ok && r.replyChan == replyChan {
		delete(ss.replyChans, rpc_args.SessionID)
	} else {
		// It has already been take out by others
	}
	ss.mu.Unlock()
	return
}

// exec will be called with ss.mu locked
func (ss *SessionServer) HandleAppliedCommand(rpc_args *RPCSessionArgs, c interface{}, exec func(interface{}, interface{}) interface{}) {
	ss.mu.Lock()
	var replyChan chan RPCSessionReply
	replyChanWithSeq, ok := ss.replyChans[rpc_args.SessionID]
	if ok && replyChanWithSeq.seq == rpc_args.Seq {
		replyChan = replyChanWithSeq.replyChan
	} else {
		replyChan = nil
	}
	// Hold the lock to make sure that replyChan will not be closed by others
	sessionReply, ok := ss.replyBuf[rpc_args.SessionID]
	if ok && sessionReply.seq >= rpc_args.Seq {
		// It has been executed.
		if replyChan != nil {
			close(replyChan)
		}
		ss.mu.Unlock()
		return
	}
	reply := RPCSessionReply{
		Err:   RPCOK,
		Reply: exec(c, rpc_args.Args),
	}
	// Fill the buffer immediately to avoid multiple execution of the same request
	ss.replyBuf[rpc_args.SessionID] = replyWithSeq{
		seq:   rpc_args.Seq,
		reply: reply,
	}
	if replyChan != nil {
		replyChan <- reply // non-blocking
	}
	ss.mu.Unlock()
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ss SessionServer

	kv          map[string]string
	lastApplied int

	quit chan struct{}
}

func raft_start(c interface{}, rpc_args *RPCSessionArgs, ss *SessionServer) {
	kv := c.(*KVServer)
	// TODO: Search in the log first to avoid appending the same request multiple times to the Raft log.
	_, _, isLeader := kv.rf.Start(*rpc_args)
	if !isLeader {
		ss.mu.Lock()
		replyChanWithSeq, ok := ss.replyChans[rpc_args.SessionID]
		if ok && replyChanWithSeq.seq == rpc_args.Seq {
			replyChanWithSeq.replyChan <- RPCSessionReply{
				Err: RPCErrWrongLeader,
			}
		}
		ss.mu.Unlock()
	}
}

func (kv *KVServer) Get(rpc_args *RPCSessionArgs, reply *RPCSessionReply) {
	// Your code here.
	// It seems that the pointer in RPCSessionArgs will become concrete struct
	get_args := rpc_args.Args.(GetArgs)
	DPrintf("%d: RPC Get, %d %d, %s\n", kv.me, rpc_args.SessionID, rpc_args.Seq, get_args.Key)
	*reply = kv.ss.HandleRPC(rpc_args, raft_start, kv, true)
}

func (kv *KVServer) PutAppend(rpc_args *RPCSessionArgs, reply *RPCSessionReply) {
	// Your code here.
	put_args := rpc_args.Args.(PutAppendArgs)
	DPrintf("%d: RPC PutAppend, %d %d, %s (%s) (%s)", kv.me, rpc_args.SessionID, rpc_args.Seq, put_args.Op, put_args.Key, put_args.Value)
	*reply = kv.ss.HandleRPC(rpc_args, raft_start, kv, false)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	DPrintf("Killing %d\n", kv.me)
	close(kv.quit)
	kv.rf.Kill()
	DPrintf("%d: Killed\n", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RPCSessionArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(map[string]string{})
	labgob.Register(map[int64]RPCSessionReply{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	ss := &kv.ss
	ss.replyChans = make(map[int64]replyChanWithSeq)

	snapshot := persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) == 0 {
		kv.kv = make(map[string]string)
		ss.replyBuf = make(map[int64]replyWithSeq)
		kv.lastApplied = kv.rf.LogBaseIndex
	} else {
		kv.installSnapshot(kv.rf.LogBaseIndex, snapshot)
	}

	kv.quit = make(chan struct{})
	ss.quit = kv.quit

	go kv.Run()

	return kv
}

func execGet(c interface{}, a interface{}) interface{} {
	kv := c.(*KVServer)
	args := a.(GetArgs)
	var getReply GetReply
	if v, ok := kv.kv[args.Key]; ok {
		getReply = GetReply{
			Err:   OK,
			Value: v,
		}
	} else {
		getReply = GetReply{
			Err: ErrNoKey,
		}
	}
	return getReply
}

func execPutAppend(c interface{}, a interface{}) interface{} {
	kv := c.(*KVServer)
	args := a.(PutAppendArgs)
	if args.Op == "Append" {
		if v, ok := kv.kv[args.Key]; ok {
			v += args.Value
			kv.kv[args.Key] = v
		} else {
			kv.kv[args.Key] = args.Value
		}
	} else if args.Op == "Put" {
		kv.kv[args.Key] = args.Value
	} else {
		log.Fatalf("Unknown operator: %s\n", args.Op)
	}
	return nil
}

func (kv *KVServer) handleAppliedCommand(msg *raft.ApplyMsg) {
	ss := &kv.ss
	rpc_args := msg.Command.(RPCSessionArgs)
	t := reflect.TypeOf(rpc_args.Args)
	if t == reflect.TypeOf(GetArgs{}) {
		args := rpc_args.Args.(GetArgs)
		DPrintf("%d: Get (%s), %d %d, applied\n", kv.me, args.Key, rpc_args.SessionID, rpc_args.Seq)
		ss.HandleAppliedCommand(&rpc_args, kv, execGet)
	} else if t == reflect.TypeOf(PutAppendArgs{}) {
		args := rpc_args.Args.(PutAppendArgs)
		DPrintf("%d: %s (%s) (%s), %d %d, applied\n", kv.me, args.Op, args.Key, args.Value, rpc_args.SessionID, rpc_args.Seq)
		ss.HandleAppliedCommand(&rpc_args, kv, execPutAppend)
	} else {
		log.Fatalf("Unknown type of command: %s\n", t.Name())
	}
}

func crashIf(e error) {
	if e != nil {
		panic(e.Error())
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.kv)
	crashIf(err)
	kv.ss.mu.Lock()
	err = e.Encode(kv.ss.replyBuf)
	crashIf(err)
	kv.ss.mu.Unlock()
	return w.Bytes()
}

func (kv *KVServer) installSnapshot(index int, snapshot []byte) {
	if kv.lastApplied >= index {
		// The snapshot is not newer. Just ignore it.
		return
	}
	kv.lastApplied = index
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	err := d.Decode(&kv.kv)
	crashIf(err)

	kv.ss.mu.Lock()
	err = d.Decode(&kv.ss.replyBuf)
	crashIf(err)
	kv.ss.mu.Unlock()
}

func (kv *KVServer) Run() {
	for {
		select {
		case <-kv.quit:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if kv.lastApplied+1 != msg.CommandIndex {
					log.Fatalf("lastApplied = %d, applied command index = %d\n", kv.lastApplied, msg.CommandIndex)
				}
				kv.lastApplied = msg.CommandIndex
				kv.handleAppliedCommand(&msg)
			} else {
				if !msg.SnapshotValid {
					log.Fatalln("Applied message not command nor snapshot!")
				}
				kv.installSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}
		case persistedSize := <-kv.rf.PersistedSizeCh:
			if kv.maxraftstate == -1 || persistedSize < kv.maxraftstate {
				break
			}
			kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshot())
		}
	}
}
