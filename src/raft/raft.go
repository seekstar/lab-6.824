package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const heartbeat_interval = time.Millisecond * 100

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Structs for internal use
type ServerState struct {
	me       int
	term     int
	isLeader bool
}

type VoteReq struct {
	args  *RequestVoteArgs
	reply chan RequestVoteReply
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type PutCmdReply struct {
	index    int
	term     int
	isLeader bool
}
type PutCmdReq struct {
	cmd   interface{}
	reply chan PutCmdReply
}

type AppendEntriesArgs struct {
	// leader’s term
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success   bool
	NextIndex int
}

type AppendEntriesArgsReply struct {
	args  *AppendEntriesArgs
	reply chan AppendEntriesReply
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	randGen *rand.Rand
	log     []LogEntry // log[0].Term is the term of the last log entry in snapshot
	// To support log compaction in the future.
	// Log entries whose indexes are smaller than LogBaseIndex are archived.
	LogBaseIndex int // Index of log[0]
	snapshot     []byte

	commitIndex int
	lastApplied int

	PersistedSizeCh chan int

	getStateCh        chan chan ServerState
	requestVoteCh     chan VoteReq
	putCmdCh          chan PutCmdReq
	appendEntriesChan chan AppendEntriesArgsReply
	installSnapshotCh chan *InstallSnapshotChItem
	applyEntryCh      chan *ApplyMsg // non-block if not quit
	applySnapshotCh   chan *ApplyMsg // non-block if not quit
	quit              chan struct{}
	quit_done         chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	replyCh := make(chan ServerState)
	select {
	case <-rf.quit:
		return 0, false
	case rf.getStateCh <- replyCh:
	}
	select {
	case <-rf.quit:
		return 0, false
	case reply := <-replyCh:
		return reply.term, reply.isLeader
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	DPrintf("%d: Persisting currentTerm = %d, votedFor = %d, log_base_index = %d, log = %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.LogBaseIndex, rf.log)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.LogBaseIndex)
	e.Encode(rf.log)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	for {
		select {
		case <-rf.PersistedSizeCh:
		case rf.PersistedSizeCh <- rf.persister.RaftStateSize():
			return
		}
	}
}

//
// restore previously persisted state.
//
// Return false if succeed, true if fail
func (rf *Raft) readPersist(state []byte, snapshot []byte) bool {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return false
	}
	// Your code here (2C).
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log_base_index int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log_base_index) != nil ||
		d.Decode(&log) != nil {
		return true
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.LogBaseIndex = log_base_index
	rf.log = log
	rf.snapshot = snapshot
	DPrintf("%d: readPersist: currentTerm = %d, votedFor = %d, log_base_index = %d, log = %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.LogBaseIndex, rf.log)
	return false
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

type SnapshotArgs struct {
	Term     int
	Index    int
	Snapshot []byte
}

// Installing snapshot should never be rejected,
// because the snapshot is always committed.
type InstallSnapshotChItem struct {
	snapshot *SnapshotArgs
	apply    bool
	reply    chan struct{}
}

func (rf *Raft) InstallSnapshotRaw(args *InstallSnapshotChItem) {
	snapshot := args.snapshot
	DPrintf("%d: InstallSnapshotRaw, index = %d\n", rf.me, snapshot.Index)
	if args.snapshot.Index <= rf.LogBaseIndex {
		DPrintf("%d: An old snapshot\n", rf.me)
		select {
		case <-rf.quit:
		case args.reply <- struct{}{}:
		}
		return
	}
	rf.snapshot = snapshot.Snapshot
	if snapshot.Index-rf.LogBaseIndex < len(rf.log) && (snapshot.Term == 0 || rf.log[snapshot.Index-rf.LogBaseIndex].Term == snapshot.Term) {
		rf.log = rf.log[snapshot.Index-rf.LogBaseIndex:]
	} else {
		rf.log = []LogEntry{{Term: snapshot.Term}}
	}
	rf.LogBaseIndex = snapshot.Index
	rf.commitIndex = MaxInt(rf.commitIndex, snapshot.Index)
	rf.lastApplied = MaxInt(rf.lastApplied, snapshot.Index)
	rf.persist()
	if args.apply {
		select {
		case <-rf.quit:
			return
		case rf.applySnapshotCh <- &ApplyMsg{false, nil, 0, true, rf.snapshot, rf.log[0].Term, rf.LogBaseIndex}:
		}
	}
	select {
	case <-rf.quit:
	case args.reply <- struct{}{}:
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("%d: Snapshot, index = %d\n", rf.me, index)
	reply := make(chan struct{})
	select {
	case <-rf.quit:
		fmt.Printf("%d: Snapshot called while being killed! How to handle this?\n", rf.me)
		return
	case rf.installSnapshotCh <- &InstallSnapshotChItem{
		&SnapshotArgs{
			0, // Won't be required
			index,
			snapshot,
		},
		false,
		reply,
	}:
	}
	DPrintf("%d: Snapshot function waiting for reply of %d\n", rf.me, index)
	select {
	case <-rf.quit:
		fmt.Printf("%d: Snapshot called while being killed! How to handle this?\n", rf.me)
		return
	case <-reply:
	}
	DPrintf("%d: Snapshot function for %d returns\n", rf.me, index)
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, _ *struct{}) {
	DPrintf("%d: RPC InstallSnapshot, index = %d\n", rf.me, args.Index)
	reply := make(chan struct{})
	rf.installSnapshotCh <- &InstallSnapshotChItem{args, true, reply}
	<-reply
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // currentTerm, for candidate to update itself
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d: RPC RequestVote, candidate %d, term %d\n", rf.me, args.CandidateId, args.Term)
	replyCh := make(chan RequestVoteReply)
	rf.requestVoteCh <- VoteReq{args, replyCh}
	*reply = <-replyCh
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	DPrintf("%d: Start(%v)\n", rf.me, command)
	replyCh := make(chan PutCmdReply)
	select {
	case <-rf.quit:
		DPrintf("%d: Start aborted\n", rf.me)
		return -1, -1, false
	case rf.putCmdCh <- PutCmdReq{command, replyCh}:
	}
	DPrintf("%d: Start(%v) waiting for reply\n", rf.me, command)
	select {
	case <-rf.quit:
		DPrintf("%d: Start aborted\n", rf.me)
		return -1, -1, false
	case reply := <-replyCh:
		DPrintf("%d: Start(%v) index = %d, term = %d, isLeader = %v\n", rf.me, command, reply.index, reply.term, reply.isLeader)
		return reply.index, reply.term, reply.isLeader
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("Killing %d\n", rf.me)
	close(rf.quit)
	<-rf.quit_done
	DPrintf("%d: Killed\n", rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.randGen = rand.New(rand.NewSource(int64(me)))
	rf.log = []LogEntry{{Term: 0}}
	rf.LogBaseIndex = 0
	rf.snapshot = nil

	rf.PersistedSizeCh = make(chan int, 1)

	rf.getStateCh = make(chan chan ServerState)
	rf.requestVoteCh = make(chan VoteReq)
	rf.putCmdCh = make(chan PutCmdReq)
	rf.appendEntriesChan = make(chan AppendEntriesArgsReply)
	rf.installSnapshotCh = make(chan *InstallSnapshotChItem)
	rf.applyEntryCh = make(chan *ApplyMsg)
	rf.applySnapshotCh = make(chan *ApplyMsg)
	rf.quit = make(chan struct{})
	rf.quit_done = make(chan struct{})

	// initialize from state persisted before a crash
	if rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot()) {
		fmt.Printf("%d: Fail to read persistend data!\n", rf.me)
		return nil
	}
	rf.lastApplied = rf.LogBaseIndex
	rf.commitIndex = rf.LogBaseIndex

	go rf.Applier(applyCh)
	go rf.do(StateFollower)

	return rf
}

func (rf *Raft) LastLogIndex() int {
	return rf.LogBaseIndex + len(rf.log) - 1
}
func (rf *Raft) LastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}
func (rf *Raft) LogUpToDate(args *RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	}
	lastTerm := rf.LastLogTerm()
	if lastTerm < args.LastLogTerm {
		return true
	}
	if lastTerm > args.LastLogTerm {
		return false
	}
	return rf.LastLogIndex() <= args.LastLogIndex
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// For candidates and leaders, if rf.votedFor == -1, then currentTerm is updated,
// and they should convert to followers.
func (rf *Raft) handleVoteRequest(args *RequestVoteArgs) (reply RequestVoteReply) {
	DPrintf("%d: handleVoteRequest: Term = %d, CandidateId = %d, LastLogIndex = %d, LastLogTerm = %d, currentTerm = %d, votedFor = %d ",
		rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm,
		rf.currentTerm, rf.votedFor)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("%d: Refuse for outdated term. currentTerm = %d\n", rf.me, rf.currentTerm)
		return
	}
	if rf.currentTerm < args.Term {
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf("%d: Refuse because already voted for %d\n", rf.me, rf.votedFor)
		return
	}
	reply.VoteGranted = rf.LogUpToDate(args)
	if reply.VoteGranted {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		DPrintf("%d: granted\n", rf.me)
	} else {
		DPrintf("%d: Refuse for outdated log\n", rf.me)
	}
	return
}

func (rf *Raft) sendWaitVote(server int, args *RequestVoteArgs, refused chan int, granted chan int, abort chan struct{}) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		select {
		case <-abort:
		case refused <- 0:
		}
		return
	}
	if reply.VoteGranted {
		select {
		case <-abort:
		case granted <- reply.Term:
		}
	} else {
		select {
		case <-abort:
		case refused <- reply.Term:
		}
	}
}

func MinInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func MaxInt(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) Applier(applyCh chan ApplyMsg) {
	var msgs []*ApplyMsg
	var snapshot *ApplyMsg = nil
	update_snapshot := func(msg *ApplyMsg) {
		if snapshot == nil || msg.SnapshotIndex > snapshot.SnapshotIndex {
			DPrintf("%d: Applier: Snapshot %d to be applied\n", rf.me, msg.SnapshotIndex)
			snapshot = msg
		}
	}
	for {
		if len(msgs) == 0 && snapshot == nil {
			select {
			case <-rf.quit:
				return
			case msg := <-rf.applyEntryCh:
				DPrintf("%d: Applier: %d %v to be applied\n", rf.me, msg.CommandIndex, msg.Command)
				msgs = append(msgs, msg)
			case msg := <-rf.applySnapshotCh:
				update_snapshot(msg)
			}
		}
		if snapshot != nil {
			for len(msgs) != 0 && msgs[0].CommandIndex <= snapshot.SnapshotIndex {
				DPrintf("%d: Applier: %d %v canceled\n", rf.me, msgs[0].CommandIndex, msgs[0].Command)
				msgs = msgs[1:]
			}
			select {
			case <-rf.quit:
				return
			case applyCh <- *snapshot:
				DPrintf("%d: Applier: Snapshot %d applied\n", rf.me, snapshot.SnapshotIndex)
				snapshot = nil
			case msg := <-rf.applyEntryCh:
				DPrintf("%d: Applier: %d %v to be applied\n", rf.me, msg.CommandIndex, msg.Command)
				msgs = append(msgs, msg)
			case msg := <-rf.applySnapshotCh:
				update_snapshot(msg)
			}
		} else {
			select {
			case <-rf.quit:
				return
			case applyCh <- *msgs[0]:
				DPrintf("%d: Applier: %d %v applied\n", rf.me, msgs[0].CommandIndex, msgs[0].Command)
				msgs = msgs[1:]
			case msg := <-rf.applyEntryCh:
				DPrintf("%d: Applier: %d %v to be applied\n", rf.me, msg.CommandIndex, msg.Command)
				msgs = append(msgs, msg)
			case msg := <-rf.applySnapshotCh:
				update_snapshot(msg)
			}
		}
	}
}

func (rf *Raft) ApplyCmds() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		// fmt.Printf("%d: ApplyCmds: log = %v, rf.lastApplied = %d, rf.log_base_index = %d\n", rf.me, rf.log, rf.lastApplied, rf.log_base_index)
		cmd := rf.log[rf.lastApplied-rf.LogBaseIndex].Command
		DPrintf("%d: ApplyCmds: Sending %d %v to applier\n", rf.me, rf.lastApplied, cmd)
		select {
		case <-rf.quit:
			DPrintf("%d: ApplyCmds: Quit\n", rf.me)
			return
		case rf.applyEntryCh <- &ApplyMsg{true, cmd, rf.lastApplied, false, nil, 0, 0}:
			DPrintf("%d: ApplyCmds: Sent %d %v to applier\n", rf.me, rf.lastApplied, cmd)
		}
	}
}

func (rf *Raft) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rf.logger.Printf("%d: _appendEntries\n", rf.me)
	reply.Term = rf.currentTerm
	if rf.LogBaseIndex+len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		return
	}
	if args.PrevLogIndex < rf.LogBaseIndex {
		// The content in snapshot is all committed and not overridable
		if args.PrevLogIndex+len(args.Entries) <= rf.LogBaseIndex {
			reply.Success = true
			reply.NextIndex = rf.LogBaseIndex + 1
			return
		}
		args.Entries = args.Entries[(rf.LogBaseIndex - args.PrevLogIndex):]
		args.PrevLogIndex = rf.LogBaseIndex
		args.PrevLogTerm = rf.log[0].Term
	}
	i := args.PrevLogIndex - rf.LogBaseIndex
	if rf.log[i].Term != args.PrevLogTerm {
		reply.Success = false
		// rf.log = rf.log[:i]
		// rf.persist()
		return
	}
	i++
	overlap := MinInt(len(rf.log)-i, len(args.Entries))
	j := 0
	for j < overlap {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}
	// fmt.Printf("%d: rf.log = %v, args.Entries = %v\n", rf.me, rf.log, args.Entries)
	// To avoid the outdated AppendEntries RPC delete the commited log entires
	if j < len(args.Entries) {
		// fmt.Printf("%d: rf.log[:i] = %v, args.Entries[j:] = %v\n", rf.me, rf.log[:i], args.Entries[j:])
		rf.log = append(rf.log[:i], args.Entries[j:]...)
		rf.persist()
	}
	// rf.logger.Printf("%d: args.LeaderCommit is %d\n", rf.me, args.LeaderCommit)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = MinInt(args.LeaderCommit, rf.LogBaseIndex+len(rf.log)-1)
		// rf.logger.Printf("Now commitIndex of %d is %d\n", rf.me, rf.commitIndex)
	}
	rf.ApplyCmds()
	reply.Success = true
	reply.NextIndex = rf.LogBaseIndex + i + len(args.Entries) - j
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("%d: AppendEntries RPC from %d\n", rf.me, args.LeaderId)
	replyChan := make(chan AppendEntriesReply)
	rf.appendEntriesChan <- AppendEntriesArgsReply{args, replyChan}
	// fmt.Printf("%d: RPC AppendEntries: sent to raft server\n", rf.me)
	*reply = <-replyChan
	// fmt.Printf("%d: RPC AppendEntries: return\n", rf.me)
}

func heartbeatTrigger(heartbeat chan struct{}, quit chan struct{}) {
	for {
		select {
		case <-quit:
			return
		case heartbeat <- struct{}{}:
		}
		time.Sleep(heartbeat_interval)
	}
}

// [3, 9) * heartbeat_interval
func (rf *Raft) genRandElectionTimeout() time.Duration {
	rf.mu.Lock()
	ret := (rf.randGen.Float64()*6 + 3) * float64(heartbeat_interval)
	rf.mu.Unlock()
	// rf.logger.Printf("Generated election timeout: %f\n", ret)
	return time.Duration(ret)
}

func (rf *Raft) electionTrigger(electionFire chan struct{}, refresh chan struct{}, quit chan struct{}) {
	for {
		select {
		case <-time.After(rf.genRandElectionTimeout()):
			electionFire <- struct{}{}
		case <-refresh:
		case <-quit:
			return
		}
	}
}

type IntPair struct {
	first  int
	second int
}

type Replicator struct {
	rf             *Raft
	currentTerm    int
	me             int
	to             int
	need_replicate chan struct{}
	quit           chan struct{}

	matchIndex chan IntPair
	higherTerm chan int
	closed     chan struct{}
}

func MakeReplicator(rf *Raft, to int, quit chan struct{}, matchIndex chan IntPair, higherTerm chan int, closed chan struct{}) (r *Replicator) {
	return &Replicator{
		rf,
		rf.currentTerm,
		rf.me,
		to,
		make(chan struct{}, 1),
		quit,
		matchIndex,
		higherTerm,
		closed,
	}
}

func (r *Replicator) NeedReplicate() {
	select {
	case r.need_replicate <- struct{}{}:
	default:
	}
}

func (r *Replicator) run() {
	heartbeat := make(chan struct{})
	go heartbeatTrigger(heartbeat, r.quit)

	var mu sync.Mutex
	r.rf.mu.Lock()
	nextIndex := r.rf.LastLogIndex() + 1
	r.rf.mu.Unlock()
	initialNextIndex := nextIndex

	replicate := func() {
		args := &AppendEntriesArgs{
			Term:     r.currentTerm,
			LeaderId: r.me,
		}
		mu.Lock()
		nextIndexLocal := nextIndex
		mu.Unlock()

		r.rf.mu.Lock()
		if nextIndexLocal <= r.rf.LogBaseIndex {
			snapshot_last_term_local := r.rf.log[0].Term
			log_base_index_local := r.rf.LogBaseIndex
			snapshot_local := r.rf.snapshot
			r.rf.mu.Unlock()
			ok := r.rf.peers[r.to].Call(
				"Raft.InstallSnapshot",
				&SnapshotArgs{
					snapshot_last_term_local,
					log_base_index_local,
					snapshot_local,
				},
				&struct{}{},
			)
			if ok {
				mu.Lock()
				nextIndex = log_base_index_local + 1
				mu.Unlock()
			}
			// retry
			r.NeedReplicate()
			return
		}
		args.PrevLogIndex = nextIndexLocal - 1
		args.PrevLogTerm = r.rf.log[args.PrevLogIndex-r.rf.LogBaseIndex].Term

		args.LeaderCommit = r.rf.commitIndex
		// It does not affect correctness if we only take reference here.
		// But it might lead to race condition if it becomes a follower and
		// the log is modified by the new leader while sending the RPC.
		args.Entries = make([]LogEntry, len(r.rf.log)-(nextIndexLocal-r.rf.LogBaseIndex))
		copy(args.Entries, r.rf.log[nextIndexLocal-r.rf.LogBaseIndex:])
		r.rf.mu.Unlock()

		DPrintf("%d: Replicating to %d, Term = %d, LeaderCommit = %d, PrevLogIndex = %d, PrevLogTerm = %d, Entries = %v\n", r.me, r.to, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries)

		// Use go routine to make sure that replicator could close quickly without being blocked by RPC
		reply := AppendEntriesReply{}
		ok := r.rf.peers[r.to].Call("Raft.AppendEntries", args, &reply)
		if !ok {
			return
		}
		if reply.Success {
			nextIndexLocal = reply.NextIndex
			mu.Lock()
			if nextIndexLocal > nextIndex {
				nextIndex = nextIndexLocal
			}
			mu.Unlock()

			matchIndex := nextIndexLocal - 1
			DPrintf("Replicator %d of %d: matchIndex is %d\n", r.to, r.me, matchIndex)
			select {
			case <-r.quit:
				return
			case r.matchIndex <- IntPair{r.to, matchIndex}:
			}
			return
		}
		if reply.Term > r.currentTerm {
			// Discovers server with higher term.
			select {
			case <-r.quit:
			case r.higherTerm <- reply.Term:
			}
			return
		}
		delta := initialNextIndex - nextIndexLocal
		if delta == 0 {
			nextIndexLocal -= 1
		} else if nextIndexLocal <= delta {
			// To reduce unnecessary InstallSnapshot RPC
			nextIndexLocal = 1
		} else {
			nextIndexLocal -= delta
		}
		mu.Lock()
		nextIndex = nextIndexLocal
		mu.Unlock()
		DPrintf("Replicator %d of %d: Log inconsistency, decrease nextIndex to %d and retry\n", r.to, r.me, nextIndexLocal)
		r.NeedReplicate()
	}

	sent := false
	for {
		select {
		case <-r.quit:
			r.closed <- struct{}{}
			DPrintf("%d: Replicator %d closed\n", r.me, r.to)
			return
		case <-heartbeat:
			if sent {
				sent = false
				break
			}
			// Use go routine to make sure that the heartbeats are sent periodically even when the RPC call does not return.
			go replicate()
		case <-r.need_replicate:
			sent = true
			go replicate()
		}
	}
}

const (
	StateFollower int = iota
	StateCandidate
	StateLeader
	StateQuit
)

func (rf *Raft) doFollower() int {
	DPrintf("%d: doFollower, currentTerm = %d\n", rf.me, rf.currentTerm)
	electionFire := make(chan struct{})
	refreshCh := make(chan struct{}, 1)
	quit := make(chan struct{})
	go rf.electionTrigger(electionFire, refreshCh, quit)
	refresh := func() {
		DPrintf("%d: Refresh election timer\n", rf.me)
		select {
		case refreshCh <- struct{}{}:
		default:
		}
	}
	for {
		DPrintf("%d(Follower): Listening\n", rf.me)
		select {
		case <-rf.quit:
			close(quit)
			return StateQuit
		case <-electionFire:
			// Time out, starts election
			close(quit)
			return StateCandidate
		case req := <-rf.getStateCh:
			DPrintf("%d: I am a follower, currentTerm = %d\n", rf.me, rf.currentTerm)
			select {
			case <-rf.quit:
				close(quit)
				return StateQuit
			case req <- ServerState{rf.me, rf.currentTerm, false}:
			}
		case req := <-rf.requestVoteCh:
			reply := rf.handleVoteRequest(req.args)
			if reply.VoteGranted {
				refresh()
			}
			req.reply <- reply
		case req := <-rf.putCmdCh:
			select {
			case <-rf.quit:
				close(quit)
				return StateQuit
			case req.reply <- PutCmdReply{-1, -1, false}:
			}
		case req := <-rf.appendEntriesChan:
			args := req.args
			DPrintf("%d: Message received from Leader %d\n", rf.me, args.LeaderId)
			var reply AppendEntriesReply
			if rf.currentTerm > args.Term {
				reply.Term = rf.currentTerm
				reply.Success = false
			} else {
				refresh()
				if rf.currentTerm < args.Term {
					rf.updateTerm(args.Term)
				}
				reply.Term = rf.currentTerm
				rf.appendEntries(args, &reply)
			}
			req.reply <- reply
		case args := <-rf.installSnapshotCh:
			rf.InstallSnapshotRaw(args)
		}
	}
}
func (rf *Raft) doCandidate() int {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("%d: doCandidate, currentTerm = %d\n", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.LastLogIndex(),
		LastLogTerm:  rf.LastLogTerm(),
	}
	refused := make(chan int)
	granted := make(chan int)
	abort := make(chan struct{})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendWaitVote(i, args, refused, granted, abort)
	}
	grantedCnt := 1

	electionFire := time.After(rf.genRandElectionTimeout())

	for {
		DPrintf("%d(Candidate): Listening\n", rf.me)
		select {
		case <-rf.quit:
			close(abort)
			return StateQuit
		case <-electionFire:
			close(abort)
			return StateCandidate
		case req := <-rf.getStateCh:
			DPrintf("%d: I am a candidate, currentTerm = %d\n", rf.me, rf.currentTerm)
			select {
			case <-rf.quit:
				close(abort)
				return StateQuit
			case req <- ServerState{rf.me, rf.currentTerm, false}:
			}
		case req := <-rf.requestVoteCh:
			req.reply <- rf.handleVoteRequest(req.args)
			if rf.votedFor != rf.me {
				// A newer term, convert to follower
				close(abort)
				return StateFollower
			}
		case req := <-rf.putCmdCh:
			select {
			case <-rf.quit:
				close(abort)
				return StateQuit
			case req.reply <- PutCmdReply{-1, -1, false}:
			}
		case req := <-rf.appendEntriesChan:
			if rf.currentTerm > req.args.Term {
				// Reject
				req.reply <- AppendEntriesReply{rf.currentTerm, false, 0}
				break
			}
			// New leader, convert to follower.
			close(abort)
			rf.updateTerm(req.args.Term)
			var reply AppendEntriesReply
			rf.appendEntries(req.args, &reply)
			req.reply <- reply
			return StateFollower
		case args := <-rf.installSnapshotCh:
			rf.InstallSnapshotRaw(args)
		case term := <-refused:
			if rf.currentTerm < term {
				// New term, convert to follower.
				close(abort)
				rf.updateTerm(term)
				return StateFollower
			}
		case <-granted:
			grantedCnt++
			if grantedCnt > len(rf.peers)/2 {
				// Received votes from majority of servers
				close(abort)
				return StateLeader
			}
		}
	}
}
func (rf *Raft) doLeader() int {
	DPrintf("%d: doLeader, currentTerm = %d\n", rf.me, rf.currentTerm)
	matchIndexes := make([]int, len(rf.peers))
	matchIndexes[rf.me] = rf.LastLogIndex()

	// TODO: Insert an empty log entry if no command in this term for a long time

	replicators := make([]*Replicator, 0, len(rf.peers)-1)
	matchIndexCh := make(chan IntPair)
	quit := make(chan struct{})
	higherTerm := make(chan int)
	closed := make(chan struct{})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		r := MakeReplicator(rf, i, quit, matchIndexCh, higherTerm, closed)
		go r.run()
		replicators = append(replicators, r)
	}
	abort := func() {
		DPrintf("%d: Leader aborting\n", rf.me)
		close(quit)
		// Then make sure that all replicators are closed
		closedCnt := 0
		for closedCnt != len(rf.peers)-1 {
			<-closed
			closedCnt++
		}
	}
	for {
		DPrintf("%d(Leader): Listening\n", rf.me)
		select {
		case <-rf.quit:
			abort()
			return StateQuit
		case term := <-higherTerm:
			if term > rf.currentTerm {
				rf.updateTerm(term)
			}
			abort()
			return StateFollower
		case req := <-rf.getStateCh:
			DPrintf("%d: I am the leader, currentTerm = %d\n", rf.me, rf.currentTerm)
			req <- ServerState{rf.me, rf.currentTerm, true}
		case req := <-rf.requestVoteCh:
			req.reply <- rf.handleVoteRequest(req.args)
			if rf.votedFor != rf.me {
				// A newer term. Convert to follower
				abort()
				return StateFollower
			}
		case req := <-rf.putCmdCh:
			DPrintf("%d: Putting command %d\n", rf.me, req.cmd)
			rf.mu.Lock()
			i := rf.LogBaseIndex + len(rf.log)
			reply := PutCmdReply{i, rf.currentTerm, true}
			rf.log = append(rf.log, LogEntry{rf.currentTerm, req.cmd})
			rf.persist()
			rf.mu.Unlock()
			select {
			case <-rf.quit:
				abort()
				return StateQuit
			case req.reply <- reply:
			}
			matchIndexes[rf.me] = i
			for _, r := range replicators {
				r.NeedReplicate()
			}
		case req := <-rf.appendEntriesChan:
			if rf.currentTerm > req.args.Term {
				// Reject
				req.reply <- AppendEntriesReply{rf.currentTerm, false, 0}
				break
			}
			// New leader, convert to follower.
			abort()
			rf.updateTerm(req.args.Term)
			var reply AppendEntriesReply
			rf.appendEntries(req.args, &reply)
			req.reply <- reply
			return StateFollower
		case matchIndex := <-matchIndexCh:
			if matchIndexes[matchIndex.first] >= matchIndex.second {
				break
			}
			DPrintf("%d: Updating matchIndex of %d to %d\n", rf.me, matchIndex.first, matchIndex.second)
			// It could be further optimized. But I am lazy.
			matchIndexes[matchIndex.first] = matchIndex.second
			sorted := append(make([]int, 0, len(matchIndexes)), matchIndexes...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			majorityIndex := sorted[len(rf.peers)/2]
			if majorityIndex > rf.commitIndex && (rf.log[majorityIndex-rf.LogBaseIndex].Term == rf.currentTerm || majorityIndex == sorted[0]) {
				// fmt.Printf("%d: rf.mu.Lock\n", rf.me)
				rf.mu.Lock()
				// fmt.Printf("%d: rf.mu.Locked\n", rf.me)
				rf.commitIndex = majorityIndex
				rf.ApplyCmds()
				rf.mu.Unlock()
				// fmt.Printf("%d: rf.mu.Unlocked\n", rf.me)
			}
		case args := <-rf.installSnapshotCh:
			DPrintf("%d: InstallSnapshotArgs received\n", rf.me)
			rf.mu.Lock()
			rf.InstallSnapshotRaw(args)
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) do(state int) {
	for {
		switch state {
		case StateFollower:
			state = rf.doFollower()
		case StateCandidate:
			state = rf.doCandidate()
		case StateLeader:
			state = rf.doLeader()
		}
		if state == StateQuit {
			break
		}
	}
	close(rf.quit_done)
}
