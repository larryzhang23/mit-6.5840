package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	shardId := shardcfg.Key2Shard(key)
	ms := 100 * time.Millisecond
	for {
		cfg := ck.sck.Query()
		_, servers, ok := cfg.GidServers(shardId)
		if !ok {
			log.Fatal("shardId is not store in any group servers\n")
		}
		clerk := shardgrp.MakeClerk(ck.clnt, servers)
		//utils.DPrintf("cfg is %v for get req (key %v, shardId %v), servers %v\n", cfg, servers, shardId, servers)
		value, version, err := clerk.Get(key)
		if err == rpc.ErrWrongGroup || err == shardrpc.ErrNoResp {
			time.Sleep(ms)
		} else {
			return value, version, err
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	
	shardId := shardcfg.Key2Shard(key)
	ms := 100 * time.Millisecond
	lostReplyBefore := false
	for {
		cfg := ck.sck.Query()
		_, servers, ok := cfg.GidServers(shardId)
		if !ok {
			log.Fatal("shardId is not store in any group servers\n")
		}
		clerk := shardgrp.MakeClerk(ck.clnt, servers)
		//utils.DPrintf("cfg is %v for put req (key %v, shard %v), servers %v\n", cfg, key, shardId, servers)
		err := clerk.Put(key, value, version)
		if err == rpc.ErrWrongGroup || err == shardrpc.ErrNoResp {
			if err == shardrpc.ErrNoResp {
				lostReplyBefore = true 
			}
			time.Sleep(ms)
		} else {
			// transform the error to maybe because the put might be successful but the reply is lost when it sends before
			if err == rpc.ErrVersion && lostReplyBefore {
				return rpc.ErrMaybe
			}
			return err
		}
	}
}
