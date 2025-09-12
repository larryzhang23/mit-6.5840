package shardgrp

import (
	"bytes"
	//"sort"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
	"6.5840/shardkv1/utils"
)


type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu sync.Mutex
	store map[string]KVServerValue
	shard map[shardcfg.Tshid]ShardInfo
}

type ShardInfo struct {
	Num shardcfg.Tnum
	Keys map[string]bool
	Frozen bool
}

type KVServerValue struct {
	Value string
	Version rpc.Tversion
}


func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if r, ok := req.(rpc.GetArgs); ok {
		reply := rpc.GetReply{}
		key := r.Key
		// check if the shard belongs to the group
		shardId := shardcfg.Key2Shard(key)
		if shardInfo, ok := kv.shard[shardId]; !ok || shardInfo.Frozen {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
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
	} else if r, ok := req.(rpc.PutArgs); ok {
		reply := rpc.PutReply{}
		key := r.Key
		version := r.Version
		newVal := r.Value
		// check if the shard belongs to the group
		shardId := shardcfg.Key2Shard(key)
		if shardInfo, ok := kv.shard[shardId]; !ok || shardInfo.Frozen {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		if valVersion, ok := kv.store[key]; ok {
			currVersion := valVersion.Version
			if version == currVersion {
				kv.store[key] = KVServerValue{Value: newVal, Version: currVersion + 1}
				kv.shard[shardId].Keys[key] = true
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			if version != 0 {
				reply.Err = rpc.ErrNoKey
			} else {
				kv.store[key] = KVServerValue{Value: newVal, Version: 1}
				kv.shard[shardId].Keys[key] = true
				reply.Err = rpc.OK
			}
		}
		return reply
	} else if r, ok := req.(shardrpc.FreezeShardArgs); ok {
		reply := shardrpc.FreezeShardReply{}
		// check if it is a valid freeze shard request
		shardInfo, ok := kv.shard[r.Shard]
		if !ok || shardInfo.Num > r.Num {
			// tmp := make([]int, 0, len(kv.shard))
			// for k := range kv.shard {
			// 	tmp = append(tmp, int(k))
			// }
			// sort.Ints(tmp)
			// utils.DPrintf("reject freeze shard from req %v by ok %v, shardInfo num %v, containing shardIds %v\n", r, ok, shardInfo.Num, tmp)
			reply.Err = shardrpc.ErrOutdatedCfg
			return reply
		}
	
		kv.shard[r.Shard] = ShardInfo{Num: shardInfo.Num, Keys: shardInfo.Keys, Frozen: true}
		shardState := make(map[string]KVServerValue)
		for k := range shardInfo.Keys {
			shardState[k] = kv.store[k]
		}
		if len(shardState) > 0 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(shardState)
			reply.State = w.Bytes()
		}
		
		reply.Err = rpc.OK
		reply.Num = shardInfo.Num
		return reply
	} else if r, ok := req.(shardrpc.InstallShardArgs); ok {
		reply := shardrpc.InstallShardReply{}
		// check if it is a valid freeze shard request
		shardInfo, ok := kv.shard[r.Shard]
		if ok && shardInfo.Num > r.Num {
			// tmp := make([]int, 0, len(kv.shard))
			// for k := range kv.shard {
			// 	tmp = append(tmp, int(k))
			// }
			// sort.Ints(tmp)
			// utils.DPrintf("reject install shard from req(shard %v, num %v) by ok %v, shardInfo num %v, containing shardIds %v\n", r.Shard, r.Num, ok, shardInfo.Num, tmp)
			reply.Err = shardrpc.ErrOutdatedCfg
			return reply
		}
	
		keys := make(map[string]bool)
		if len(r.State) > 0 {
			reader := bytes.NewBuffer(r.State)
			d := labgob.NewDecoder(reader)
			var state map[string]KVServerValue
			d.Decode(&state)
			for k, v := range state {
				keys[k] = true 
				kv.store[k] = v
			}
		}
		kv.shard[r.Shard] = ShardInfo{Keys: keys, Num: r.Num}
		// if term, isLeader := kv.rsm.Raft().GetState(); isLeader {
		// 	utils.DPrintf("Term: %v, install shard %v on gid %v server %v; shard: %v\n", term, r.Shard, kv.gid, kv.me, kv.shard)
		// }
		reply.Err = rpc.OK
		return reply
	} else if r, ok := req.(shardrpc.DeleteShardArgs); ok {
		reply := shardrpc.DeleteShardReply{}
		// check if it is a valid freeze shard request
		shardInfo, ok := kv.shard[r.Shard]
		if !ok || shardInfo.Num > r.Num {
			// tmp := make([]int, 0, len(kv.shard))
			// for k := range kv.shard {
			// 	tmp = append(tmp, int(k))
			// }
			// sort.Ints(tmp)
			// utils.DPrintf("reject delete shard from req %v by ok %v, shardInfo num %v, containing shardIds %v\n", r, ok, shardInfo.Num, tmp)
			reply.Err = shardrpc.ErrOutdatedCfg
			return reply
		}
		delete(kv.shard, r.Shard)
		for k := range shardInfo.Keys {
			delete(kv.store, k)
		}
		// if term, isLeader := kv.rsm.Raft().GetState(); isLeader { 
		// 	utils.DPrintf("Term: %v, delete shard %v on gid %v server %v\n", term, r.Shard, kv.gid, kv.me)
		// }
		reply.Err = rpc.OK
		return reply
	}
	// never reach here
	return nil
}


func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.shard)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	w := bytes.NewBuffer(data)
	d := labgob.NewDecoder(w)
	var store map[string]KVServerValue
	var shard map[shardcfg.Tshid]ShardInfo
	d.Decode(&store)
	d.Decode(&shard)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = store 
	kv.shard = shard
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
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
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err == rpc.OK {
		val := result.(rpc.PutReply)
		reply.Err = val.Err 
	} else {
		reply.Err = err
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err == rpc.OK {
		val := result.(shardrpc.FreezeShardReply)
		reply.State = val.State
		reply.Num = val.Num
		reply.Err = val.Err
	} else {
		reply.Err = err
	}
	
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err == rpc.OK {
		val := result.(shardrpc.InstallShardReply)
		reply.Err = val.Err
	} else {
		reply.Err = err
	}
	
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err == rpc.OK {
		val := result.(shardrpc.DeleteShardReply)
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
	utils.DPrintf("gid %v, server id %v is killed\n", kv.gid, kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.store = make(map[string]KVServerValue)
	kv.shard = make(map[shardcfg.Tshid]ShardInfo)
	// the first shardgroup initalizes to own all shards (setting in the lab)
	if gid == shardcfg.Gid1 {
		for shardId := range shardcfg.NShards {
			kv.shard[shardcfg.Tshid(shardId)] = ShardInfo{Num: shardcfg.NumFirst, Keys: make(map[string]bool)}
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
