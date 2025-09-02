package kvraft

import (
	"time"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu sync.Mutex
	lastSeenLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.lastSeenLeader = -1
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
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
	for reply.Err == "" || reply.Err == rpc.ErrWrongLeader {
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
		time.Sleep(100 * time.Millisecond)
	}
	// never reach here
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
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
	
	// the outside for-loop is for the case when the servers are under leader selection or network partision
	for reply.Err == "" || reply.Err == rpc.ErrWrongLeader {
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
		time.Sleep(100 * time.Millisecond)
	}
	// never reach here
	return reply.Err
}
