package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey string
	lockId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l 
	lk.lockId = kvtest.RandValue(8)
	_, _, err := lk.ck.Get(l)
	if err == rpc.ErrNoKey {
		lk.ck.Put(l, "0", 0)
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.lockKey)
		if val == "0" {
			err = lk.ck.Put(lk.lockKey, lk.lockId, version)
			switch err {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				val, _, _ := lk.ck.Get(lk.lockKey)
				if val == lk.lockId {
					return 
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	val, version, _ := lk.ck.Get(lk.lockKey) 
	if val != lk.lockId {
		log.Fatal("Call release before acquire")
	}
	err := lk.ck.Put(lk.lockKey, "0", version)
	if err == rpc.ErrVersion {
		log.Fatal("lock key is corructed in the server side")
	}
}
