package rsm

import (
	"bytes"
	"sync"
	"time"

	"github.com/google/uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/labgob"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id string
	Req any
}

type SnapshotWithIndex struct {
	Snapshot []byte 
	LastIncludedIndex int
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	results map[string]any
	// a channel to tell stuck submit request that the raft service is shutdown
	stopCh chan struct{}
	// largest log index, tracing for snapshot
	appliedIndex int
	// last snapshot index
	snapshotIndex int 
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if persister.SnapshotSize() > 0 {
		snapshotData := persister.ReadSnapshot()
		snapshotWithIndex := rsm.decodeSnapshot(snapshotData)
		rsm.sm.Restore(snapshotWithIndex.Snapshot)
		rsm.appliedIndex = snapshotWithIndex.LastIncludedIndex
		rsm.snapshotIndex = snapshotWithIndex.LastIncludedIndex
	}

	rsm.results = make(map[string]any)
	rsm.stopCh = make(chan struct{})
	go rsm.reader()
	if rsm.maxraftstate > -1 {
		go rsm.makeSnapshot()
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{Id: uuid.NewString(), Req: req}

	_, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	ms := time.Duration(10) * time.Millisecond
	
	var result any
	for {
		select {
		case <- rsm.stopCh:
			return rpc.ErrWrongLeader, nil
		case <- time.After(ms):
			rsm.mu.Lock()
			if val, ok := rsm.results[op.Id]; ok {
				result = val
				rsm.mu.Unlock()
				return rpc.OK, result
			}
			rsm.mu.Unlock()
			// detect if the leadership is changed; cant use the isLeader because if the server is again becoming the leader with a larger term
			// and there is no new commits, the old one will be stuck here for a long time
			currTerm, _ := rsm.rf.GetState()
			if currTerm != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}


func (rsm *RSM) reader() {
	for m := range rsm.applyCh {
		if m.CommandValid {
			command := m.Command.(Op)
			rsm.mu.Lock()
			result := rsm.sm.DoOp(command.Req)
			rsm.appliedIndex = m.CommandIndex
			rsm.results[command.Id] = result
			rsm.mu.Unlock()
		} else if m.SnapshotValid {
			snapshotWithIndex := rsm.decodeSnapshot(m.Snapshot)
			rsm.mu.Lock()
			rsm.sm.Restore(snapshotWithIndex.Snapshot)
			rsm.appliedIndex = m.SnapshotIndex
			rsm.mu.Unlock()
		}
	}
	close(rsm.stopCh)
}

func (rsm *RSM) makeSnapshot() {
	ms := time.Duration(10) * time.Millisecond
	for {
		select {
		case <- rsm.stopCh:
			return
		case <- time.After(ms):
			rsm.mu.Lock()
			if rsm.appliedIndex > 0 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				snapshotWithIndex := rsm.encodeSnapshot(snapshot)
				rsm.rf.Snapshot(rsm.appliedIndex, snapshotWithIndex)
				rsm.snapshotIndex = rsm.appliedIndex
			}
			rsm.mu.Unlock()
		}
	}
}

func (rsm *RSM) encodeSnapshot(snapshot []byte) []byte {
	// lock acquired when called
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshotWithIndex := SnapshotWithIndex{Snapshot: snapshot, LastIncludedIndex: rsm.appliedIndex}
	e.Encode(snapshotWithIndex)
	return w.Bytes()
}

func (rsm *RSM) decodeSnapshot(snapshotWithIndexData []byte) *SnapshotWithIndex {
	// lock acquired when called
	r := bytes.NewBuffer(snapshotWithIndexData)
	d := labgob.NewDecoder(r)
	var snapshotWithIndex SnapshotWithIndex
	d.Decode(&snapshotWithIndex)
	return &snapshotWithIndex
}