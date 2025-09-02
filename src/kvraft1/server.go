package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex
	store map[string]KVServerValue
}

type KVServerValue struct {
	Value string
	Version rpc.Tversion
}

type Req struct {
	ReqType int // 0 is get, 1 is put
	Key string
	Value string // only used by put
	Version rpc.Tversion // only used by put
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	r := req.(Req)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if r.ReqType == 0 {
		reply := rpc.GetReply{}
		key := r.Key
		if valVersion, ok := kv.store[key]; ok {
			value := valVersion.Value
			version := valVersion.Version
			reply.Value = value 
			reply.Version = version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply
	} else {
		reply := rpc.PutReply{}
		key := r.Key
		version := r.Version
		newVal := r.Value
		if valVersion, ok := kv.store[key]; ok {
			currVersion := valVersion.Version
			if version == currVersion {
				kv.store[key] = KVServerValue{Value: newVal, Version: currVersion + 1}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			if version != 0 {
				reply.Err = rpc.ErrNoKey
			} else {
				kv.store[key] = KVServerValue{Value: newVal, Version: 1}
				reply.Err = rpc.OK
			}
		}
		return reply
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	w := bytes.NewBuffer(data)
	d := labgob.NewDecoder(w)
	var store map[string]KVServerValue
	d.Decode(&store)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = store 
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := Req{ReqType: 0, Key: args.Key}
	err, result := kv.rsm.Submit(req)
	if err == rpc.OK {
		val := result.(rpc.GetReply)
		reply.Err = val.Err 
		reply.Value = val.Value
		reply.Version = val.Version
	} else {
		reply.Err = err
	}
	
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := Req{ReqType: 1, Key: args.Key, Value: args.Value, Version: args.Version}
	err, result := kv.rsm.Submit(req)
	if err == rpc.OK {
		val := result.(rpc.PutReply)
		reply.Err = val.Err 
	} else {
		reply.Err = err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(Req{})

	kv := &KVServer{me: me}
	
	kv.store = make(map[string]KVServerValue)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	// Testcase potential issue or an undiscovered bug?
	// For test TestSnapshotRPC4C, there might be low chance that we replicate logs on majority of servers 
	// but timeout before commit, in this case the test will stuck and we need a no-op command to commit the 
	// the past log
	return []tester.IService{kv, kv.rsm.Raft()}
}
