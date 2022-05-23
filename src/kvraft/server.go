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

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type SessionReply struct {
	seq   int64
	reply interface{}
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mu         sync.Mutex
	replyChans map[int64]chan SessionReply
	replyBuf   map[int64]SessionReply

	kv          map[string]string
	lastApplied int

	quit chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%d: RPC Get, %d %d, %s\n", kv.me, args.SessionID, args.Seq, args.Key)
	kv.mu.Lock()
	if buf, ok := kv.replyBuf[args.SessionID]; ok {
		if buf.seq == args.Seq {
			*reply = buf.reply.(GetReply)
			kv.mu.Unlock()
			return
		} else if buf.seq > args.Seq {
			kv.mu.Unlock()
			*reply = GetReply{
				Err: ErrObsoleteRequest,
			}
			return
		}
	}
	replyChan := make(chan SessionReply, 1)
	if c, ok := kv.replyChans[args.SessionID]; ok {
		close(c)
	}
	kv.replyChans[args.SessionID] = replyChan
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		kv.mu.Lock()
		if r, ok := kv.replyChans[args.SessionID]; ok && r == replyChan {
			delete(kv.replyChans, args.SessionID)
		} else {
			// It has already been taken out.
			// It's okay to not listen to it. It's non-blocking anyway.
		}
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-kv.quit:
		reply.Err = ErrWrongLeader
	case r, ok := <-replyChan:
		if !ok || r.seq != args.Seq {
			reply.Err = ErrReplacedRequest
		} else {
			*reply = r.reply.(GetReply)
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%d: RPC PutAppend, %d %d, %s (%s) (%s)", kv.me, args.SessionID, args.Seq, args.Op, args.Key, args.Value)
	kv.mu.Lock()
	if buf, ok := kv.replyBuf[args.SessionID]; ok {
		if buf.seq == args.Seq {
			*reply = buf.reply.(PutAppendReply)
			kv.mu.Unlock()
			return
		} else if buf.seq > args.Seq {
			kv.mu.Unlock()
			*reply = PutAppendReply{
				Err: OK,
			}
			return
		}
	}
	replyChan := make(chan SessionReply, 1)
	if c, ok := kv.replyChans[args.SessionID]; ok {
		close(c)
	}
	kv.replyChans[args.SessionID] = replyChan
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		kv.mu.Lock()
		if r, ok := kv.replyChans[args.SessionID]; ok && r == replyChan {
			delete(kv.replyChans, args.SessionID)
		} else {
			// It has already been taken out.
			// It's okay to not listen to it. It's non-blocking anyway.
		}
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-kv.quit:
		reply.Err = ErrWrongLeader
	case r, ok := <-replyChan:
		if !ok || r.seq != args.Seq {
			reply.Err = ErrReplacedRequest
		} else {
			*reply = r.reply.(PutAppendReply)
		}
	}
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
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(map[string]string{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.replyChans = make(map[int64]chan SessionReply)
	kv.replyBuf = make(map[int64]SessionReply)

	snapshot := persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) == 0 {
		kv.kv = make(map[string]string)
		kv.lastApplied = kv.rf.LogBaseIndex
	} else {
		kv.installSnapshot(kv.rf.LogBaseIndex, snapshot)
	}

	kv.quit = make(chan struct{})

	go kv.Run()

	return kv
}

func (kv *KVServer) handleAppliedCommand(msg *raft.ApplyMsg) {
	t := reflect.TypeOf(msg.Command)
	if t == reflect.TypeOf(GetArgs{}) {
		args := msg.Command.(GetArgs)
		kv.mu.Lock()
		sessionReply, ok := kv.replyBuf[args.SessionID]
		if !ok || sessionReply.seq != args.Seq {
			var reply GetReply
			if v, ok := kv.kv[args.Key]; ok {
				reply = GetReply{
					Err:   OK,
					Value: v,
				}
			} else {
				reply = GetReply{
					Err: ErrNoKey,
				}
			}
			sessionReply = SessionReply{
				seq:   args.Seq,
				reply: reply,
			}
			kv.replyBuf[args.SessionID] = sessionReply
		}
		replyChan, ok := kv.replyChans[args.SessionID]
		if ok {
			delete(kv.replyChans, args.SessionID)
			replyChan <- sessionReply // non-blocking
		}
		kv.mu.Unlock()
	} else if t == reflect.TypeOf(PutAppendArgs{}) {
		args := msg.Command.(PutAppendArgs)
		kv.mu.Lock()
		sessionReply, ok := kv.replyBuf[args.SessionID]
		if !ok || sessionReply.seq != args.Seq {
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
			reply := PutAppendReply{
				Err: OK,
			}
			sessionReply = SessionReply{
				seq:   args.Seq,
				reply: reply,
			}
			kv.replyBuf[args.SessionID] = sessionReply
		}
		replyChan, ok := kv.replyChans[args.SessionID]
		if ok {
			delete(kv.replyChans, args.SessionID)
			replyChan <- sessionReply // non-blocking
		}
		kv.mu.Unlock()
	} else {
		log.Fatalf("Unknown type of command: %s\n", t.Name())
	}
}

func (kv *KVServer) installSnapshot(index int, snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	err := d.Decode(&kv.kv)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if kv.lastApplied > index {
		log.Fatalf("Snapshot index (%d) < last applied index (%d)\n", index, kv.lastApplied)
	}
	kv.lastApplied = index
}

func (kv *KVServer) Run() {
	for {
		select {
		case <-kv.quit:
			return
		case msg := <-kv.applyCh:
			DPrintf("%d: %v applied\n", kv.me, msg.Command)
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
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kv)
			snapshot := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, snapshot)
		}
	}
}
