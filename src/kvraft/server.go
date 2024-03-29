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
	Seq   int64
	Reply RPCSessionReply
}

type replyChanWithSeq struct {
	seq       int64
	replyChan chan RPCSessionReply
}

type SessionServer struct {
	mu         sync.Mutex
	ReplyChans map[int64]replyChanWithSeq
	ReplyBuf   map[int64]replyWithSeq
	Quit       chan struct{}
}

func InitSessionServer(ss *SessionServer, quit chan struct{}) {
	ss.ReplyChans = make(map[int64]replyChanWithSeq)
	ss.ReplyBuf = make(map[int64]replyWithSeq)
	ss.Quit = quit
}

func (ss *SessionServer) EncodeTo(e *labgob.LabEncoder) {
	ss.mu.Lock()
	err := e.Encode(ss.ReplyBuf)
	crashIf(err)
	ss.mu.Unlock()
}

func (ss *SessionServer) DecodeFrom(d *labgob.LabDecoder) {
	ss.mu.Lock()
	ss.ReplyBuf = make(map[int64]replyWithSeq)
	err := d.Decode(&ss.ReplyBuf)
	crashIf(err)
	ss.mu.Unlock()
}

func (ss *SessionServer) checkBuf(rpc_args *RPCSessionArgs, reply *RPCSessionReply, withRet bool) bool {
	ss.mu.Lock()
	buf, ok := ss.ReplyBuf[rpc_args.SessionID]
	ss.mu.Unlock()
	if ok {
		if buf.Seq == rpc_args.Seq {
			*reply = buf.Reply
			return true
		} else if buf.Seq > rpc_args.Seq {
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

func (ss *SessionServer) HandleRPC(rpc_args *RPCSessionArgs, do func(interface{}, *RPCSessionArgs, *SessionServer) int, c interface{}, withRet bool) (reply RPCSessionReply) {
	if ss.checkBuf(rpc_args, &reply, withRet) {
		return
	}
	replyChan := make(chan RPCSessionReply, 1)
	ss.mu.Lock()
	if original, ok := ss.ReplyChans[rpc_args.SessionID]; ok {
		original.replyChan <- RPCSessionReply{Err: RPCErrReplacedRequest}
	}
	ss.ReplyChans[rpc_args.SessionID] = replyChanWithSeq{
		seq:       rpc_args.Seq,
		replyChan: replyChan,
	}
	ss.mu.Unlock()
	reply.Err = do(c, rpc_args, ss)
	if reply.Err != RPCOK {
		ss.mu.Lock()
		replyChanWithSeq, ok := ss.ReplyChans[rpc_args.SessionID]
		if ok && replyChanWithSeq.seq == rpc_args.Seq {
			delete(ss.ReplyChans, rpc_args.SessionID)
		}
		ss.mu.Unlock()
		return
	}
	select {
	case <-ss.Quit:
		reply.Err = RPCErrKilled
	case reply = <-replyChan:
	}
	// replyChan will be removed from ss.replyChans by its writer
	return
}

func (ss *SessionServer) HandleAppliedCommand(rpc_args *RPCSessionArgs, c interface{}, exec func(interface{}, interface{}) interface{}, withRet bool) {
	ss.mu.Lock()
	var replyChan chan RPCSessionReply
	replyChanWithSeq, replyChanOk := ss.ReplyChans[rpc_args.SessionID]
	sessionReply, ok := ss.ReplyBuf[rpc_args.SessionID]
	if replyChanOk && replyChanWithSeq.seq == rpc_args.Seq {
		replyChan = replyChanWithSeq.replyChan
		// Hold the lock to make sure that replyChan will not be closed by others
		var reply RPCSessionReply
		if ok && sessionReply.Seq >= rpc_args.Seq {
			// It has been executed.
			if sessionReply.Seq == rpc_args.Seq {
				reply = sessionReply.Reply
			} else if withRet {
				reply = RPCSessionReply{Err: RPCErrObsoleteRequest}
			} else {
				reply = RPCSessionReply{Err: RPCOK}
			}
		} else {
			reply = RPCSessionReply{
				Err:   RPCOK,
				Reply: exec(c, rpc_args.Args),
			}
			// Fill the buffer immediately to avoid multiple execution of the same request
			ss.ReplyBuf[rpc_args.SessionID] = replyWithSeq{
				Seq:   rpc_args.Seq,
				Reply: reply,
			}
		}
		replyChan <- reply // non-blocking
		delete(ss.ReplyChans, rpc_args.SessionID)
	} else {
		if !ok || sessionReply.Seq < rpc_args.Seq {
			reply := RPCSessionReply{
				Err:   RPCOK,
				Reply: exec(c, rpc_args.Args),
			}
			// Fill the buffer immediately to avoid multiple execution of the same request
			ss.ReplyBuf[rpc_args.SessionID] = replyWithSeq{
				Seq:   rpc_args.Seq,
				Reply: reply,
			}
		}
	}
	ss.mu.Unlock()
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ss          SessionServer
	kv          map[string]string
	lastApplied int
	quit        chan struct{}
}

func raft_start(c interface{}, rpc_args *RPCSessionArgs, ss *SessionServer) int {
	kv := c.(*KVServer)
	// TODO: Search in the log first to avoid appending the same request multiple times to the Raft log.
	_, _, isLeader := kv.rf.Start(*rpc_args)
	if !isLeader {
		return RPCErrWrongLeader
	}
	return RPCOK
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.quit = make(chan struct{})
	InitSessionServer(&kv.ss, kv.quit)

	snapshot := persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) == 0 {
		kv.kv = make(map[string]string)
		kv.lastApplied = kv.rf.LogBaseIndex
	} else {
		kv.installSnapshot(kv.rf.LogBaseIndex, snapshot)
	}

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
		ss.HandleAppliedCommand(&rpc_args, kv, execGet, true)
	} else if t == reflect.TypeOf(PutAppendArgs{}) {
		ss.HandleAppliedCommand(&rpc_args, kv, execPutAppend, false)
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
	kv.ss.EncodeTo(e)
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
	kv.ss.DecodeFrom(d)
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
