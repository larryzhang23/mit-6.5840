package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	// "log"
	"bytes"
	"log"
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

	// tick sign, only used when it is not a leader 
	tick bool 
	// count how many acknowlage from peers, used when it is a candidate
	votesCount int 
	// help find the nextIdx when appendEntries fails
	termLastEntry map[int]int
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

	// assume lock is acquired when the function is called
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	raftState := w.Bytes()
	// TODO: change second parameter in (3D)
	rf.persister.Save(raftState, nil)
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
	for i, log := range rf.logs {
		rf.termLastEntry[log.Term] = i
	}
	DPrintf("Server %v read persistence done\n", rf.me)
	
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


func (rf *Raft) updateTermAndStateIfPossible(term int) bool {
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reqTerm := args.Term
	reqLastLogTerm := args.LastLogTerm
	reqLastLogIdx := args.LastLogIdx 
	candidateId := args.CandidateId
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// implement rule 2 for all servers in the paper
	updatePersist := rf.updateTermAndStateIfPossible(reqTerm)
	// set default to be deny voting
	reply.VoteGranted = false 
	if rf.currentTerm == reqTerm {
		// check votefor field
		if rf.voteFor == -1 || rf.voteFor == candidateId {
			rfLastLogIdx := len(rf.logs) - 1
			rfLastLogTerm := rf.logs[rfLastLogIdx].Term
			if rfLastLogTerm < reqLastLogTerm || (rfLastLogTerm == reqLastLogTerm && rfLastLogIdx <= reqLastLogIdx) {
				reply.VoteGranted = true
				rf.voteFor = candidateId
				updatePersist = true
				// reset timer
				rf.tick = true
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
		// if more than a half votegranted, rf.state will be leader 
		// if it is a outdated leader, rf.state will be changed back to follower
		// thus, only need to process while it is still a candidate
		// implement rule 2 for all servers
		updatePersist := rf.updateTermAndStateIfPossible(reply.Term)
		if rf.state == Candidate {
			if reply.VoteGranted {
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
	nextLogIdx := len(rf.logs)
	if nextLogIdx == 0 {
		DPrintf("Leader %v logs becomes empty which is impossible!", rf.me)
	}
	for i := range rf.nextIdx {
		if i == rf.me {
			continue 
		}
		rf.nextIdx[i] = nextLogIdx
		rf.matchIdx[i] = 0
	}
	// start heartbeats goroutine
	go rf.syncup()
	DPrintf("%v becomes a leader\n", rf.me)
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
	defer rf.mu.Unlock()
	// handle the case the server is not the leader 
	term = rf.currentTerm
	if rf.state != Leader {
		return index, term, false
	} 
	// start the agreement service 
	newLog := Log{Term: term, Command: command}
	rf.logs = append(rf.logs, newLog)
	index = len(rf.logs) - 1
	// update last entry of term if necessary
	rf.termLastEntry[term] = index
	// update persist
	rf.persist()

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
	// when we call this function, default is the lock is acquired
	DPrintf("%v start a new term(%v) to be a candidate", rf.me, rf.currentTerm + 1)
	rf.currentTerm++
	rf.voteFor = rf.me 
	rf.state = Candidate
	rfLastLogIdx := len(rf.logs) - 1
	rfLastLogTerm := rf.logs[rfLastLogIdx].Term
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		ms := 600 + (rand.Int63() % 600)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader {
			// rf.tick is set in the rpc AppendEntries
			if rf.tick {
				rf.tick = false 
			} else {
				// DPrintf("%v times out at term %v\n", rf.me, rf.currentTerm)
				rf.handleLeaderSelection()
			}
		} 
		rf.mu.Unlock()
	}
}

func (rf *Raft) syncup() {
	ms := 100
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return 
		}
		rf.handleAppendEntries()
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	// implement rule 2 for all servers
	updatePersist := rf.updateTermAndStateIfPossible(args.Term)
	if !updatePersist && args.Term != rf.currentTerm {
		DPrintf("Follower %v (term %v) and Leader %v (term %v)\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}
	reply.Success = false 
	//
	// Set reply.XLen to -1 telling leader it is a failed request(if it is not updated in the following logic) because of term
	// This is for unreliable network where the follower rejects the leader because of term but later on 
	// the same leader was selected with higher term 
	//
	reply.XLen = -1
	reply.XIndex = -1
	reply.XTerm = -1
	if args.Term == rf.currentTerm {
		rf.tick = true 
		// fix those who transformed from candidate or expired leader in rf.UpdateTermAndStateIfPossible
		if rf.voteFor == -1 {
			rf.voteFor = args.LeaderId
			updatePersist = true
		}
		// since the term of follower doesn't conflict with the requested leader, we set the XLen to the logs length
		reply.XLen = len(rf.logs)
		// compare prevLogIdx and prevLogTerm
		if len(rf.logs) > args.PrevLogIdx {
			if rf.logs[args.PrevLogIdx].Term == args.PrevLogTerm {
				// set reply to be successful
				reply.Success = true 
				// append new entries
				if len(args.Entries) > 0 {
					// can't just simply append, because the prevLogIdx maybe smaller than the actual rf.logs which contain uncommited logs, so the code in the next line is one of the bug taking me a while to fix
					// rf.logs = append(rf.logs, args.Entries...)

					// update last entry of terms 
					newTermLastEntry := make(map[int]int)
					// remove the truncated terms in the map 
					newTermLastEntry[args.PrevLogTerm] = args.PrevLogIdx
					for k, v := range rf.termLastEntry {
						if k < args.PrevLogTerm {
							newTermLastEntry[k] = v
						} 
					}
					// add new logs and update last entry of terms
					rf.logs = rf.logs[:args.PrevLogIdx + 1]
					for i, log := range args.Entries {
						newTermLastEntry[log.Term] = i + args.PrevLogIdx + 1
						rf.logs = append(rf.logs, log)
					}
					rf.termLastEntry = newTermLastEntry
					updatePersist = true 
				}
				// update commitedIdx
				if args.LeaderCommit > rf.commitedIdx {
					lastCommitedIdx := rf.commitedIdx
					rf.commitedIdx = min(len(rf.logs) - 1, args.LeaderCommit)
					DPrintf("Follower %v updates commitedIdx from %v to %v at term %v\n", rf.me, lastCommitedIdx, rf.commitedIdx, rf.currentTerm)
					go rf.commitLogs(lastCommitedIdx + 1, rf.commitedIdx)
				}
			} else {
				term := rf.logs[args.PrevLogIdx].Term
				reply.XTerm = term 
				lastTerm := 0
				// get the first index of the conflict term so that leader can use it to skip the current term
				for t := range rf.termLastEntry {
					if t < term && t > lastTerm {
						lastTerm = t 
					}
				}
				reply.XIndex = rf.termLastEntry[lastTerm] + 1
				DPrintf("AppendEntries Failed from Leader %v; Follower %v return first index %v of the conflict term %v\n", args.LeaderId, rf.me, reply.XIndex, term)
			}
		}
	}
	if updatePersist {
		rf.persist()
	}
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		updatePersist := rf.updateTermAndStateIfPossible(reply.Term)
		if rf.state == Leader {
			// handle response
			if reply.Success {
				// update matchIdx, be careful that the leader might accept client requests at the same time
				if len(args.Entries) > 0 {
					matchIdx := args.PrevLogIdx + len(args.Entries)
					rf.matchIdx[server] = matchIdx
					rf.nextIdx[server] = matchIdx + 1
					// update the commitIndex if the log is in current term
					if rf.commitedIdx < matchIdx && rf.logs[matchIdx].Term == rf.currentTerm {
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
							go rf.commitLogs(lastCommited + 1, rf.commitedIdx)
						}
					}
				}
			} else {
				if reply.XTerm > 0 {
					if idx, ok := rf.termLastEntry[reply.XTerm]; ok {
						if idx == -1 {
							DPrintf("Leader %v's last entry of term %v is -1 which is impossible\n", rf.me, reply.XTerm)
						}
						rf.nextIdx[server] = idx + 1 
					} else {
						rf.nextIdx[server] = reply.XIndex
						if reply.XIndex == 0 {
							DPrintf("Follower %v's first entry of term %v is 0 which is impossible(term > 1, index > 1 or term = 0 should succeed) \n", server, reply.XTerm)
						}
					}
				} else if reply.XLen > 0 {
					rf.nextIdx[server] = reply.XLen
				} else {
					// for debug purpose
					DPrintf("Follower %v(term %v)'s XLen %v, XTerm %v XIndex %v rejects leader request because of term. Don't update anything and just retry", server, reply.Term, reply.XLen, reply.XTerm, reply.XIndex)
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
	// assuem lock is acuired when the function is called
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		prevLogTerm := rf.logs[rf.nextIdx[i] - 1].Term
		var entries []Log
		if rf.nextIdx[i] < len(rf.logs) {
			// When the server is a outdated leader and the real leader call us,
			// it might change rf.logs during the gob encoding time(ok := rf.peers[server].Call("Raft.AppendEntries", args, reply))
			entries = make([]Log, len(rf.logs[rf.nextIdx[i]:]))
			copy(entries, rf.logs[rf.nextIdx[i]:])
		}
		
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIdx: rf.nextIdx[i] - 1,
			PrevLogTerm: prevLogTerm,
			LeaderCommit: rf.commitedIdx,
			Entries: entries,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}


func (rf *Raft) commitLogs(startIdx, endIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for ;startIdx <= endIdx; startIdx++ {
		rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: rf.logs[startIdx].Command, CommandIndex: startIdx}
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
	rf.termLastEntry = map[int]int{0: 0}
	rf.logs = append(rf.logs, Log{Term: rf.currentTerm})

	for range peers {
		rf.nextIdx = append(rf.nextIdx, 1)
		rf.matchIdx = append(rf.matchIdx, 0)
	}

	// initialize from state persisted before a crash	
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}