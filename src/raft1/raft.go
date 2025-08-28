package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"log"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// channel to communicate with server
	applyCh chan raftapi.ApplyMsg

	// server state
	state State
	
	// persistent 
	currentTerm int 
	voteFor int 
	logs []Log 

	// volatile, apply all time
	commitedIdx int
	lastApplied int

	// volatile, only used when it is a leader
	nextIdx []int
	matchIdx []int

	// snapshot
	snapshot *SnapshotBody
	// decide if we have a new snapshot to apply to the service
	lastAppliedSnapshotIndex int 

	// count how many acknowlage from peers, used when it is a candidate
	votesCount int 
	// help find the nextIdx when appendEntries fails, here the index is the logs array index not the log index
	termLastEntry map[int]int
	// heartbeat channel
	heartbeatCh chan struct{}
	// replicate log after receiving request from client
	sendLogCh chan struct{}
	// A channel to inform short time running goroutine the server is closed
	stopCh chan struct{}
}

type State int 

const (
	Leader State = iota 
	Candidate 
	Follower
)

type Log struct {
	Term int
	Command any
	Index int 
}

type SnapshotBody struct {
	Data []byte
	LastIncludedIndex int
	LastIncludedTerm int
}

/*
########################
public API / tester API
########################
*/

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	idx := rf.indexToInd(index)
	logItem := rf.logs[idx]
	// for debug
	if logItem.Index != index {
		log.Fatalf("log index %v different from snapshot index %v\n", logItem.Index, index)
	}
	rf.snapshot = &SnapshotBody{Data: snapshot, LastIncludedIndex: index, LastIncludedTerm: logItem.Term}
	// update lastAppliedSnapshotIndex
	rf.lastAppliedSnapshotIndex = index
	// shrink the rf.logs, the last item in snapshot now becomes the sentinel
	rf.shrinkLog("right", idx)
	// update termLastEntry
	rf.updateTermLastEntry()
	rf.persist()
	DPrintf("[goroutineId | %v]: server %v (state %v) create snapshot until index %v; shrink log to length %v\n", getGID(), rf.me, rf.state, index, len(rf.logs))
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	// handle the case the server is killed 
	if rf.killed() {
		return index, term, false 
	}
	rf.mu.Lock()
	// handle the case the server is not the leader 
	term = rf.currentTerm
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, false
	} 
	// start the agreement service 
	index = rf.getLastLog().Index + 1
	newLog := Log{Term: term, Command: command, Index: index}
	rf.logs = append(rf.logs, newLog)
	
	// update last entry of term
	rf.termLastEntry[term] = len(rf.logs) - 1
	// update persist
	rf.persist()
	rf.mu.Unlock()
	// send logs to peers
	rf.sendLogCh <- struct{}{}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	// atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	if atomic.SwapInt32(&rf.dead, 1) == 0 {
        close(rf.stopCh)
		// don't close applyCh here
    }
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.state = Follower
	rf.currentTerm = 0 
	rf.voteFor = -1
	rf.commitedIdx = 0
	rf.lastApplied = 0
	rf.votesCount = -1
	// apply a dummy term 0 log as the initial sentinel
	rf.termLastEntry = map[int]int{0: 0}
	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Index: 0})
	// initialize heartbeat channel
	rf.heartbeatCh = make(chan struct{}, 1)
	// initialize the sendLog channel 
	rf.sendLogCh = make(chan struct{})
	rf.stopCh = make(chan struct{})

	for range peers {
		rf.nextIdx = append(rf.nextIdx, 1)
		rf.matchIdx = append(rf.matchIdx, 0)
	}

	// initialize from state persisted before a crash, including snapshots
	rf.readPersist(persister.ReadRaftState())
	
	// start election timeout goroutine
	go rf.heartbeatDaemon()
	// start applydaemon to apply logs
	go rf.applyDaemon()
	DPrintf("[goroutineId | %v]: server %v starts finish\n", getGID(), rf.me)

	return rf
}

/*
##################################
Helper function defined by the lab
##################################
*/

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// assume lock is acquired when the function is called
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	

	// save snapshot lastincludedTerm and lastIncludedIndex if there is a snapshot
	var snapshot []byte
	if rf.snapshot != nil {
		// because the tester load the snapshot directly when it restarts from crash so we can't save lastIncludedIndex and lastIncludedTerm in the snapshot
		// we save lastIncludedIndex and lastIncludedTerm in rafestate
		e.Encode(rf.snapshot.LastIncludedIndex)
		e.Encode(rf.snapshot.LastIncludedTerm)
		snapshot = rf.snapshot.Data
	}

	raftState := w.Bytes()
	rf.persister.Save(raftState, snapshot)
	
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// nil slice len() is zero
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// only called once before server is ready to provide service, so no need lock
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int 
	var voteFor int 
	var logs []Log 
	if err := d.Decode(&currentTerm); err != nil {
		log.Fatalf("Server %v load currentTerm from persister fails with err %v\n", rf.me, err)
	}
	if err := d.Decode(&voteFor); err != nil {
		log.Fatalf("Server %v load voteFor from persister fails with err %v\n", rf.me, err)
	}
	if err := d.Decode(&logs); err != nil {
		log.Fatalf("Server %v load logs from persister fails with err %v\n", rf.me, err)
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs 
	// update termLastEntry
	rf.updateTermLastEntry()
	
	// try to load snapshots
	if snapshot := rf.persister.ReadSnapshot(); len(snapshot) > 0 {
		var lastIncludedIndex int 
		var lastincludedTerm int 
		if err := d.Decode(&lastIncludedIndex); err != nil {
			log.Fatalf("Server %v load lastIncludedIndex from persister fails with err %v\n", rf.me, err)
			}
		if err := d.Decode(&lastincludedTerm); err != nil {
			log.Fatalf("Server %v load lastIncludedTerm from persister fails with err %v\n", rf.me, err)
		}
		rf.snapshot = &SnapshotBody{Data: snapshot, LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastincludedTerm}
		// set commitIdx to lastIncludedIndex 
		rf.commitedIdx = lastIncludedIndex
		// update last applied
		rf.lastApplied = lastIncludedIndex
		// update lastappliedindex because the tester will apply snapshot to the service
		rf.lastAppliedSnapshotIndex = lastIncludedIndex

		DPrintf("[goroutineId | %v]: server %v restarts with term %v and votefor %v; loads snapshot with index %v and term %v, size %v\n", getGID(), rf.me, rf.currentTerm, rf.voteFor, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm, len(snapshot))
		// DPrintf("server %v's snapshot data: %v", rf.me, rf.snapshot.Data)
	} else {
		DPrintf("[goroutineId | %v]: server %v no snapshots(snapshot size %v)\n", getGID(), rf.me, rf.persister.SnapshotSize())
	}
}

/*
#########################################
RPC definitions and some helper functions
#########################################
*/

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int 
	LastLogIdx int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool 
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// set default to be deny voting
	reply.VoteGranted = false 
	if args.Term < rf.currentTerm {
		return
	}

	reqTerm := args.Term
	reqLastLogTerm := args.LastLogTerm
	reqLastLogIdx := args.LastLogIdx 
	candidateId := args.CandidateId

	// implement rule 2 for all servers in the paper
	updatePersist := rf.updateTermAndStateFromLargerTerm(reqTerm)
	
	// check votefor field
	if rf.voteFor == -1 || rf.voteFor == candidateId {
		lastLog := rf.getLastLog()
		rfLastLogIdx := lastLog.Index
		rfLastLogTerm := lastLog.Term
		// session 5.2 & 5.4
		if rfLastLogTerm < reqLastLogTerm || (rfLastLogTerm == reqLastLogTerm && rfLastLogIdx <= reqLastLogIdx) {
			reply.VoteGranted = true
			rf.voteFor = candidateId
			updatePersist = true
			// reset timer and drop packet if there is already a heartbeat, 
			// it is safe to do it under the lock because it is a buffered channel and we have default branch
			select {
			case rf.heartbeatCh <- struct{}{}:
			default:
			}
		}
	} 
	
	// check if we need to update persisters
	if updatePersist {
		rf.persist()
	}
}

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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// another guard point to avoid handling outdated request
		if rf.currentTerm != args.Term || rf.state != Candidate {
			return ok
		}
		// if more than a half votegranted, rf.state will be leader 
		// if it is a outdated leader, rf.state will be changed back to follower
		// thus, only need to process while it is still a candidate
		// implement rule 2 for all servers
		updatePersist := rf.updateTermAndStateFromLargerTerm(reply.Term)
		if rf.state == Candidate {
			if reply.VoteGranted {
				DPrintf("[goroutineId | %v]: Candidate %v receives vote from server %v\n", getGID(), rf.me, server)
				rf.votesCount++
				majority := len(rf.peers) / 2 + 1
				if rf.votesCount + 1 >= majority {
					rf.leaderInit()
				}
			}
		}
		if updatePersist {
			rf.persist()
		}
	}
	return ok
}

func (rf *Raft) leaderInit() {
	rf.state = Leader
	nextLogIdx := rf.getLastLog().Index + 1
	for i := range rf.peers {
		if i == rf.me {
			continue 
		}
		rf.nextIdx[i] = nextLogIdx
		// if there is snapshot, and since snapshot is at most until commited index, we can set matchIdx to the last entry of the snapshot if exists
		rf.matchIdx[i] = 0
	}
	// start heartbeats goroutine
	go rf.leaderDaemon()
	DPrintf("[goroutineId | %v]: %v becomes a leader\n", getGID(), rf.me)
}

func (rf *Raft) handleLeaderSelection() {
	// start leader electtion 
	// when we call this function, default is the lock is acquired
	DPrintf("[goroutineId | %v]: %v start a new term(%v) from state %v to be a candidate\n", getGID(), rf.me, rf.currentTerm + 1, rf.state)
	rf.currentTerm++
	rf.voteFor = rf.me 
	rf.state = Candidate
	lastLog := rf.getLastLog()
	rfLastLogIdx := lastLog.Index
	rfLastLogTerm := lastLog.Term
	rf.votesCount = 0
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogTerm: rfLastLogTerm,
		LastLogIdx: rfLastLogIdx,
	}

	// update persist
	rf.persist()

	for i := range rf.peers {
		if i == rf.me {
			continue 
		} 
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
}

type AppendEntriesArgs struct {
	Term int 
	LeaderId int 
	PrevLogIdx int 
	PrevLogTerm int 
	Entries []Log
	LeaderCommit int 
}

type AppendEntriesReply struct {
	Term int 
	Success bool
	// useful information for leader to decide nextIdx when request fails
	XTerm int 
	XIndex int 
	XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false 
	//
	// Set reply.XLen to -1 telling leader it is a failed request(if it is not updated in the following logic) because of term
	// This is for unreliable network where the follower rejects the leader because of term but later on 
	// the same leader was selected with higher term 
	//
	reply.XLen = -1
	reply.XIndex = -1
	reply.XTerm = -1

	if args.Term < rf.currentTerm {
		return
	}
	// implement rule 2 for all servers
	updatePersist := rf.updateTermAndStateFromLargerTerm(args.Term)
	// reset timer 
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
	// fix the server which is currrently in candidate state and has the same term as the leader
	if rf.voteFor != args.LeaderId {
		rf.updateStateFromTheLeaderRequest(args.LeaderId)
		updatePersist = true
	}
	// since the term of follower doesn't conflict with the requested leader, we set the XLen to the logs length
	lastLog := rf.getLastLog()
	// apply index transformation
	idx := rf.indexToInd(args.PrevLogIdx)
	// handle the out-of-order and delayed request
	if idx < 0 {
		DPrintf("follower %v receives a request that compare packets before snapshots; current snapshot lastincludedindex %v, args.PrevLogIdx %v\n", rf.me, rf.snapshot.LastIncludedIndex, args.PrevLogIdx)
		// trim the args.Entries to start after rf.snapshot.
		// debug
		if -idx - 1 < len(args.Entries) && args.Entries[-idx - 1].Term != rf.snapshot.LastIncludedTerm {
			log.Fatalf("Leader %v packet at index %v has term %v different from snapshot lastincludedindex %v's term %v\n", args.LeaderId, args.Entries[-idx-1].Index, args.Entries[-idx-1].Term, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)
		}
		args.PrevLogIdx = rf.snapshot.LastIncludedIndex
		args.PrevLogTerm = rf.snapshot.LastIncludedTerm
		trimIdx := min(-idx, len(args.Entries))
		args.Entries = args.Entries[trimIdx:]
		idx = 0
	}

	// set XLen to the length of current log so that the leader knows
	reply.XLen = lastLog.Index + 1
	if lastLog.Index >= args.PrevLogIdx {
		prevLog := rf.logs[idx]
		if prevLog.Term == args.PrevLogTerm {
			// set reply to be successful
			reply.Success = true 
			// append new entries
			if len(args.Entries) > 0 {
				// add new logs and update last entry of terms if necessary
				// shouldn't trim the logs directly in case it is an older request from the same leader but with shorter entries
				// rf.shrinkLog("left", idx + 1)
				i := 0
				for ; i + idx + 1 < len(rf.logs) && i < len(args.Entries); i++ {
					if rf.logs[i + idx + 1].Term == args.Entries[i].Term {
						continue 
					} 
					rf.shrinkLog("left", i + idx + 1)
					break
				}
				if i < len(args.Entries) {
					rf.logs = append(rf.logs, args.Entries[i:]...)
					lastLog = &rf.logs[len(rf.logs) - 1]
					rf.updateTermLastEntry()
					updatePersist = true 
				}
			}
			// update commitedIdx
			if args.LeaderCommit > rf.commitedIdx && lastLog.Index > rf.commitedIdx {
				lastCommitedIdx := rf.commitedIdx
				rf.commitedIdx = min(lastLog.Index, args.LeaderCommit)
				DPrintf("Follower %v updates commitedIdx from %v to %v at term %v\n", rf.me, lastCommitedIdx, rf.commitedIdx, rf.currentTerm)
			}
		} else {
			term := prevLog.Term
			reply.XTerm = term 
			lastTerm := 0
			// get the first index of the conflict term so that leader can use it to skip the current term
			for t := range rf.termLastEntry {
				if t < term && t > lastTerm {
					lastTerm = t 
				}
			}
			reply.XIndex = rf.logs[rf.termLastEntry[lastTerm]].Index + 1
			// DPrintf("[goroutineId | %v]: server %v rejects with lastTerm %v, and logs %v\n", getGID(), rf.me, lastTerm, rf.logs)
		}
	}
	if updatePersist {
		rf.persist()
	}
	// debug
	if !reply.Success {
		DPrintf("[goroutineId | %v]: AppendEntries Failed from Leader %v (term %v) of previndex %v, prevterm %v; Follower %v returns XLen %v, XTerm %v, XIndex %v; follower last log index %v\n", getGID(), args.LeaderId, args.Term, args.PrevLogIdx, args.PrevLogTerm, rf.me, reply.XLen, reply.XTerm, reply.XIndex, rf.getLastLog().Index)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// guard point to avoid processing outdated response
		if rf.currentTerm != args.Term || rf.state != Leader {
			return ok
		}
		updatePersist := rf.updateTermAndStateFromLargerTerm(reply.Term)
		if rf.state == Leader {
			// handle response
			if reply.Success {
				// update matchIdx, be careful that the leader might accept client requests at the same time
				matchIdx := args.PrevLogIdx + len(args.Entries)
				nextIdx := matchIdx + 1

				if matchIdx > rf.matchIdx[server] {
					rf.matchIdx[server] = matchIdx
					rf.nextIdx[server] = nextIdx
				}

				// for debug
				var lastEntry Log
				if len(args.Entries) > 0 {
					lastEntry = args.Entries[len(args.Entries) - 1]
				} else {
					lastEntry = *rf.getLastLog()
				}
				// DPrintf("[goroutineId | %v]: Request successful update follower %v nextIdx to be %v and last entry: %v; leader logs: %v\n", getGID(), server, matchIdx + 1, lastEntry, rf.logs)
				DPrintf("[goroutineId | %v]: Request successful update follower %v nextIdx to be %v and last entry: %v; leader logs length: %v\n", getGID(), server, matchIdx + 1, lastEntry, len(rf.logs))
				// update the commitIndex if the log is in current term
				if rf.commitedIdx < matchIdx && rf.logs[rf.indexToInd(matchIdx)].Term == rf.currentTerm {
					replicateCnt := 0
					majority := len(rf.peers) / 2 + 1
					for i := range rf.matchIdx {
						if i == rf.me {
							continue
						}
						if rf.matchIdx[i] >= matchIdx {
							replicateCnt++
							// more than a half agree, break
							if replicateCnt + 1 >= majority {
								break
							}
						}
					}
					// increate commitedIdx
					if replicateCnt + 1 >= majority {
						lastCommited := rf.commitedIdx
						rf.commitedIdx = matchIdx
						DPrintf("Leader %v updates commitedIdx from %v to %v at term %v\n", rf.me, lastCommited, rf.commitedIdx, rf.currentTerm)
					}
				}
			} else {
				if reply.XTerm > 0 {
					if idx, ok := rf.termLastEntry[reply.XTerm]; ok {
						rf.nextIdx[server] = rf.logs[idx].Index + 1
					} else {
						rf.nextIdx[server] = reply.XIndex
					}
				} else if reply.XLen > 0 {
					DPrintf("[goroutineId | %v]: Before update: nextIdx[%d]=%d, reply.XLen=%d", getGID(), server, rf.nextIdx[server], reply.XLen)
					rf.nextIdx[server] = reply.XLen
				} else {
					// network partision or conjection leading to late response
					DPrintf("[goroutineId | %v]: Follower %v(term %v)'s XLen %v, XTerm %v XIndex %v rejects leader request because of term. Don't update anything and just retry", getGID(), server, reply.Term, reply.XLen, reply.XTerm, reply.XIndex)
				}
			}
		}

		if updatePersist {
			rf.persist()
		}
	}
	return ok
}


func (rf *Raft) handleAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return 
	}
	// assuem lock is acuired when the function is called
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// DPrintf("for follower %v, nextIdx is %v\n", i, rf.nextIdx[i])
		// Decide to send InstallSnapshot RPC or AppendEntries RPC
		previdx := rf.indexToInd(rf.nextIdx[i] - 1)
		if previdx < 0 {
			data := make([]byte, len(rf.snapshot.Data))
			copy(data, rf.snapshot.Data)
			args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: rf.snapshot.LastIncludedIndex,
				LastIncludedTerm: rf.snapshot.LastIncludedTerm,
				Data: data,
			}
			reply := InstallSnapshotReply{}
			go rf.sendSnapshot(i, &args, &reply)
		} else {
			// add entries if necessary
			lastLog := rf.getLastLog()
			var entries []Log
			if rf.nextIdx[i] <= lastLog.Index {
				// When the server is a outdated leader and the real leader call us,
				// it might change rf.logs during the gob encoding time(ok := rf.peers[server].Call("Raft.AppendEntries", args, reply))
				entries = make([]Log, len(rf.logs[previdx+1:]))
				copy(entries, rf.logs[previdx+1:])
			}
			
			prevLogTerm := rf.logs[previdx].Term
			prevLogIndex := rf.logs[previdx].Index
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIdx: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				LeaderCommit: rf.commitedIdx,
				Entries: entries,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}


type InstallSnapshotArgs struct {
	Term int
	LeaderId  int 
	LastIncludedIndex int 
	LastIncludedTerm int 
	Data []byte 
}


type InstallSnapshotReply struct {
	Term int 
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// reset timer and drop it if there is already a heartbeat
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}

	// implement rule 2 for all servers
	updatePersist := rf.updateTermAndStateFromLargerTerm(args.Term)
	
	if rf.voteFor != args.LeaderId {
		rf.updateStateFromTheLeaderRequest(args.LeaderId)
		updatePersist = true
	}
	// first case, the snapshot is older than the one in the follower (unreliable network causing older snapshot arriving late)
	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex >= args.LastIncludedIndex {
		DPrintf("Follower %v receive a snapshot with smaller includedIndex %v than its current ones %v\n", rf.me, args.LastIncludedIndex, rf.snapshot.LastIncludedIndex)
		if updatePersist {
			rf.persist()
		}
		return 
	}

	DPrintf("Follower %v receive a snapshot with includedIndex %v from leader %v\n", rf.me, args.LastIncludedIndex, args.LeaderId)
	lastLog := rf.getLastLog()

	// all other cases, it will install snapshot and trim the logs
	// be careful to apply indexToInd before updating the snapshot, because indexToInd relies on snapshot
	idx := rf.indexToInd(args.LastIncludedIndex)
	rf.snapshot = &SnapshotBody{Data: args.Data, LastIncludedIndex: args.LastIncludedIndex, LastIncludedTerm: args.LastIncludedTerm}
	updatePersist = true 

	// second case, the snapshot is ahead of logs; remove the whole rf.logs (here we add a sentinel log)
	if args.LastIncludedIndex > lastLog.Index {
		rf.logs = []Log{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	} else if snapshotLog := rf.logs[idx]; snapshotLog.Term == args.LastIncludedTerm {
		// third case, the snapshot is prefix of the rf.logs
		DPrintf("[goroutineId | %v]: server %v trim logs to tails from startidx %v from to the end %v\n", getGID(), rf.me, idx, len(rf.logs) - 1)
		rf.shrinkLog("right", idx)
	} else {
		// fourth case, the snapshot conflicts with the rf.logs; remove the whole rf.logs (here we add a sentinel log)
		rf.logs = []Log{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}
	rf.updateTermLastEntry()

	// update commitIndex
	if rf.snapshot.LastIncludedIndex > rf.commitedIdx {
		rf.commitedIdx = rf.snapshot.LastIncludedIndex	
	}

	if updatePersist {
		rf.persist()
	}
}


func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// guard point to avoid processing outdated response
		if rf.currentTerm != args.Term || rf.state != Leader {
			return ok
		}
		updatePersist := rf.updateTermAndStateFromLargerTerm(reply.Term)
		if rf.state == Leader {
			matchIdx := args.LastIncludedIndex
			rf.matchIdx[server] = matchIdx
			rf.nextIdx[server] = matchIdx + 1
		}
		if updatePersist {
			rf.persist()
		}
	}
	return ok
}

/*
###########################################
long-running goroutines
###########################################
*/

func (rf *Raft) heartbeatDaemon() {
	ms := 600 + (rand.Int63() % 600)
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)

	for {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		select {
		case <- rf.stopCh:
			return 
		case <- timer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				// DPrintf("%v times out at term %v\n", rf.me, rf.currentTerm)
				rf.handleLeaderSelection()
			}
			rf.mu.Unlock()
		case <- rf.heartbeatCh:
		}
		ms = 600 + (rand.Int63() % 600)
		timer.Reset(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) leaderDaemon() {
	ms := 100
	for {
		select {
		case <- rf.stopCh:
		return 
		// to replicate logs from user request right away
		case <- rf.sendLogCh:
		rf.handleAppendEntries()
		case <- time.After(time.Duration(ms) * time.Millisecond):
		rf.handleAppendEntries()
		}
	}
}

func (rf *Raft) applyDaemon() {
	defer close(rf.applyCh)
	ms := time.Duration(10) * time.Millisecond
	for {
		select {
		case <- rf.stopCh:
			return 
		case <- time.After(ms):
			var snapshot []byte
			lastIncludedIndex := -1
			lastIncludedTerm := -1
			rf.mu.Lock()
			if rf.snapshot != nil && rf.lastAppliedSnapshotIndex < rf.snapshot.LastIncludedIndex {
				lastIncludedIndex = rf.snapshot.LastIncludedIndex
				lastIncludedTerm = rf.snapshot.LastIncludedTerm
				snapshot = make([]byte, len(rf.snapshot.Data))
				copy(snapshot, rf.snapshot.Data)
				rf.lastAppliedSnapshotIndex = rf.snapshot.LastIncludedIndex
			}
			rf.mu.Unlock()
			if len(snapshot) > 0 {
				rf.applyCh <- raftapi.ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: lastIncludedTerm, SnapshotIndex: lastIncludedIndex}
			}

			var copyLogs []Log
			rf.mu.Lock()
			if rf.lastApplied < rf.commitedIdx {
				sidx := rf.lastApplied + 1
				if rf.snapshot != nil {
					sidx = max(sidx, rf.snapshot.LastIncludedIndex + 1)
				} 
				if sidx <= rf.commitedIdx {
					copyLogs = make([]Log, rf.commitedIdx - sidx + 1)
					sidx, eidx := rf.indexToInd(sidx), rf.indexToInd(rf.commitedIdx)
					copy(copyLogs, rf.logs[sidx:eidx+1])
				}
				rf.lastApplied = rf.commitedIdx
			}
			rf.mu.Unlock()
			for _, logItem := range copyLogs {
				rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: logItem.Command, CommandIndex: logItem.Index}
			}
		}
	}
}

/*
###########################################
Some helper functions defined by the author
###########################################
*/


func (rf *Raft) updateTermAndStateFromLargerTerm(term int) bool {
	// assume lock acquired
	// implement rule 2 for all servers in the paper
	// return value represents if the server is updated or not(for 3C)
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
		rf.state = Follower
		rf.votesCount = -1
		return true 
	}
	return false 
}

func (rf *Raft) updateStateFromTheLeaderRequest(leaderId int) {
	rf.voteFor = leaderId
	rf.state = Follower
	rf.votesCount = -1
}


// helper function for transforming log index to the index in the logs array (for case using log compaction)
func (rf *Raft) indexToInd(logIndex int) int {
	// lock is acquired
	if rf.snapshot != nil {
		snapshotIndex := rf.snapshot.LastIncludedIndex
		// we keep a sentinel log in the index 0 of the rf.logs which is either (term 0) or the last entry of the snapshot
		// DPrintf("server %v trigger here and the value is %v(%v - %v)\n", rf.me, logIndex - snapshotIndex, logIndex, snapshotIndex)
		return logIndex - snapshotIndex
	} else {
		return logIndex
	}

}

func (rf *Raft) getLastLog() *Log {
	// because we keep the sentinel log, even when there is no "actual" log in the rf.logs, rf.logs[0] will be the last entry of the snapshot
	return &rf.logs[len(rf.logs) - 1]
}


func (rf *Raft) shrinkLog(side string, idx int) {
	// lock is acquired
	if side == "left" {
		// not including the idx
		rf.logs = rf.logs[:idx]
	} else {
		rf.logs = rf.logs[idx:]
	}

}


func (rf *Raft) updateTermLastEntry() {
	newTermLastEntry := make(map[int]int)
	for i, log := range rf.logs {
		newTermLastEntry[log.Term] = i
	}
	rf.termLastEntry = newTermLastEntry
}