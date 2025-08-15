package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"log"

	//	"6.5840/labgob"
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

	// tick sign, only used when it is not a leader 
	tick bool 
	// count how many acknowlage from peers, used when it is a candidate
	votesCount int 
}

type State int 

const (
	Leader State = iota 
	Candidate 
	Follower
)

type Log struct {
	Term int
}


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
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

}


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
	reqTerm := args.Term
	reqLastLogTerm := args.LastLogTerm
	reqLastLogIdx := args.LastLogIdx 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rfTerm := rf.currentTerm
	if rfTerm > reqTerm {
		// when the term is equal, it means either the current server is a candidate or it already votes for another candidate
		reply.Term = rfTerm 
		reply.VoteGranted = false 
		log.Printf("from %v: %v term is smaller to my term %v\n", rf.me, reqTerm, rfTerm)
	} else if rfTerm == reqTerm {
		if rf.voteFor == args.CandidateId {
			reply.Term = rfTerm
			reply.VoteGranted = true 
		} else {
			reply.Term = rfTerm
			reply.VoteGranted = false 
		}
	} else {
		rfLastLogIdx := len(rf.logs) - 1
		var rfLastLogTerm int 
		if rfLastLogIdx >= 0 {
			rfLastLogTerm = rf.logs[rfLastLogIdx].Term 
		} else {
			rfLastLogTerm = -1
		}
		if rfLastLogTerm <= reqLastLogTerm && rfLastLogIdx <= reqLastLogIdx {
			rf.currentTerm = reqTerm
			rf.voteFor = args.CandidateId
			// update beats
			rf.tick = true
			// update state
			rf.state = Follower
			reply.Term = reqTerm
			reply.VoteGranted = true
			log.Printf("%v acknowledge %v as leader\n", rf.me, args.CandidateId)
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false 
		}
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

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, resp chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		resp <- reply
	} else {
		resp <- &RequestVoteReply{VoteGranted: false, Term: -2}
	}
	return ok
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) handleLeaderSelection() {
	// start leader electtion 
	log.Printf("%v start a new term(%v) to be a candidate", rf.me, rf.currentTerm + 1)
	rf.currentTerm++
	rf.voteFor = rf.me 
	rf.state = Candidate
	lastLogIdx := len(rf.logs) - 1
	var lastLogTerm int 
	if lastLogIdx > 0 {
		lastLogTerm = rf.logs[lastLogIdx].Term
	} else {
		lastLogTerm = -1
	}
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogTerm: lastLogTerm,
		LastLogIdx: lastLogIdx,
	}

	resp := make(chan *RequestVoteReply, len(rf.peers) - 1)
	
	for i := range rf.peers {
		if i == rf.me {
			continue 
		} 
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply, resp)
	}
	respCount := 0
	votesCount := 0
	for reply := range resp {
		if reply.VoteGranted {
			votesCount++
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.state = Follower 
				return 
			}
		}
		respCount++
		if respCount == len(rf.peers) - 1 {
			break 
		}
	}
	log.Printf("%v gets votes count: %v\n", rf.me, votesCount)
	if votesCount >= int(math.Ceil(float64(len(rf.peers)) / 2)) - 1 {
		rf.state = Leader 
		// TODO: some initialization of the nextIdx and matchIdx
		nextLogIdx := lastLogIdx + 1
		for i := range rf.nextIdx {
			rf.nextIdx[i] = nextLogIdx
			rf.matchIdx[i] = -1
		}
		
		// start heartbeats goroutine
		go rf.syncup()
		log.Printf("%v becomes a leader\n", rf.me)
	} 
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		ms := 600 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.state != Leader {
			// rf.tick is set in the rpc AppendEntries
			if rf.tick {
				log.Printf("%v receive heartbeats\n", rf.me)
				rf.tick = false 
			} else {
				rf.handleLeaderSelection()
			}
		} 
		rf.mu.Unlock()
	}
}

func (rf *Raft) syncup() {
	ms := 100
	for rf.killed() == false {
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.handleAppendEntries()
		} else {
			rf.mu.Unlock()
			return 
		}
		rf.mu.Unlock()
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term 
		rf.voteFor = args.LeaderId
		rf.state = Follower
		rf.tick = true 
		reply.Term = rf.currentTerm
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else {
		if args.LeaderId == rf.voteFor || rf.voteFor == -1 {
			rf.tick = true 
			if rf.voteFor == -1 {
				rf.voteFor = args.LeaderId
			}
			reply.Term = rf.currentTerm
		} else {
			reply.Term = rf.currentTerm
		}
		
	} 
	// TODO: finish the logic of append logs, change the reply.Success
	reply.Success = true 
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, resp chan *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		resp <- reply
	} else {
		resp <- &AppendEntriesReply{Success: false, Term: -2}
	}
	return ok
}


func (rf *Raft) handleAppendEntries() {
	resp := make(chan *AppendEntriesReply, len(rf.peers) - 1)
	for i := range rf.peers {
		prevLogTerm := -1
		if rf.nextIdx[i] > 0 {
			prevLogTerm = rf.logs[rf.nextIdx[i] - 1].Term
		}
		// TODO: fix leadercommit and validate other args
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIdx: rf.nextIdx[i] - 1,
			PrevLogTerm: prevLogTerm,
			LeaderCommit: -1,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply, resp)
	}

	// TODO: handle resp, if resp 
	respCount := 0
	for peerReply := range resp {
		if peerReply.Term > rf.currentTerm { 
			rf.currentTerm = peerReply.Term
			rf.voteFor = -1
			rf.state = Follower
			// TODO: should we update tick here?
			rf.tick = true 
			break 
		}
		respCount++
		if respCount == len(rf.peers) - 1 {
			break
		}
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
	rf.state = Follower
	rf.currentTerm = 0 
	rf.voteFor = -1
	rf.commitedIdx = -1
	rf.lastApplied = -1
	// TODO: load it from persistent states
	for range peers {
		rf.nextIdx = append(rf.nextIdx, 0)
		rf.matchIdx = append(rf.matchIdx, -1)
	}


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}