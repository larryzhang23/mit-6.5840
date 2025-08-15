package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServerValue struct {
	value string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]KVServerValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.store = make(map[string]KVServerValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if valVersion, ok := kv.store[key]; ok {
		value := valVersion.value
		version := valVersion.version
		reply.Value = value 
		reply.Version = version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	key := args.Key
	newVal := args.Value
	version := args.Version
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if valVersion, ok := kv.store[key]; ok {
		currVersion := valVersion.version
		if version == currVersion {
			kv.store[key] = KVServerValue{value: newVal, version: currVersion + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if version != 0 {
			reply.Err = rpc.ErrNoKey
		} else {
			kv.store[key] = KVServerValue{value: newVal, version: 1}
			reply.Err = rpc.OK
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
