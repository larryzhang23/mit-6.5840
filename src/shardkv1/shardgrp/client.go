package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu sync.Mutex
	lastSeenLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.lastSeenLeader = -1
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	var reply rpc.GetReply
	// if we have leader history before, try old leader first
	ck.mu.Lock()
	lastSeenLeader := ck.lastSeenLeader
	ck.mu.Unlock() 
	if lastSeenLeader != -1 {
		ok := ck.clnt.Call(ck.servers[lastSeenLeader], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Value, reply.Version, reply.Err
		}
	}
	
	for idx, server := range ck.servers {
		// reinitialize reply for gob
		reply = rpc.GetReply{}
		ok := ck.clnt.Call(server, "KVServer.Get", &args, &reply)
		
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.mu.Lock()
			ck.lastSeenLeader = idx
			ck.mu.Unlock()
			return reply.Value, reply.Version, reply.Err
		}
	}
	
	reply.Err = shardrpc.ErrNoResp
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	var reply rpc.PutReply
	firstTimeSuccess := true

	ck.mu.Lock()
	lastSeenLeader := ck.lastSeenLeader
	ck.mu.Unlock() 
	if lastSeenLeader != -1 {
		ok := ck.clnt.Call(ck.servers[lastSeenLeader], "KVServer.Put", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		firstTimeSuccess = false 
	}
	
	
	for idx, server := range ck.servers {
		reply = rpc.PutReply{}
		ok := ck.clnt.Call(server, "KVServer.Put", &args, &reply)

		// if it is not the wrong leader err, return directly
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.mu.Lock()
			ck.lastSeenLeader = idx
			ck.mu.Unlock()
			if !firstTimeSuccess && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			} 
			return reply.Err
		} 
		firstTimeSuccess = false 
	}
	
	reply.Err = shardrpc.ErrNoResp
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}
	// if we have leader history before, try old leader first
	ck.mu.Lock()
	lastSeenLeader := ck.lastSeenLeader
	ck.mu.Unlock() 
	if lastSeenLeader != -1 {
		ok := ck.clnt.Call(ck.servers[lastSeenLeader], "KVServer.FreezeShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.State, reply.Err
		}
	}
	retry := 0
	for reply.Err == "" || reply.Err == rpc.ErrWrongLeader {
		for idx, server := range ck.servers {
			reply := shardrpc.FreezeShardReply{}
			ok := ck.clnt.Call(server, "KVServer.FreezeShard", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.lastSeenLeader = idx
				ck.mu.Unlock()
				return reply.State, reply.Err
			}
		}
		retry++
		if retry == 10 {
			reply.Err = shardrpc.ErrNoResp
			return reply.State, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
	// never reach here
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}
	// if we have leader history before, try old leader first
	ck.mu.Lock()
	lastSeenLeader := ck.lastSeenLeader
	ck.mu.Unlock() 
	if lastSeenLeader != -1 {
		ok := ck.clnt.Call(ck.servers[lastSeenLeader], "KVServer.InstallShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
	}
	retry := 0
	for reply.Err == "" || reply.Err == rpc.ErrWrongLeader {
		for idx, server := range ck.servers {
			reply := shardrpc.InstallShardReply{}
			ok := ck.clnt.Call(server, "KVServer.InstallShard", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.lastSeenLeader = idx
				ck.mu.Unlock()
				return reply.Err
			}
		}
		retry++
		if retry == 10 {
			reply.Err = shardrpc.ErrNoResp
			return reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}
	// if we have leader history before, try old leader first
	ck.mu.Lock()
	lastSeenLeader := ck.lastSeenLeader
	ck.mu.Unlock() 
	if lastSeenLeader != -1 {
		ok := ck.clnt.Call(ck.servers[lastSeenLeader], "KVServer.DeleteShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
	}
	retry := 0
	for reply.Err == "" || reply.Err == rpc.ErrWrongLeader {
		for idx, server := range ck.servers {
			reply := shardrpc.DeleteShardReply{}
			ok := ck.clnt.Call(server, "KVServer.DeleteShard", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.lastSeenLeader = idx
				ck.mu.Unlock()
				return reply.Err
			}
		}
		retry++
		if retry == 10 {
			reply.Err = shardrpc.ErrNoResp
			return reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Err
}
