package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"math/rand"
	"sync"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/shardkv1/utils"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	cfgKey string
	cfgKeyNext string
	// for debug
	id int
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.cfgKey = "cfg"
	sck.cfgKeyNext = "cfgNew"
	sck.id = rand.Int()
	return sck
}

// for debug
func (sck *ShardCtrler) Id() int {
	return sck.id
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	cfgStr, cfgVersionCurr, _ := sck.IKVClerk.Get(sck.cfgKey)
	cfgStrNewInPast, _, _ := sck.IKVClerk.Get(sck.cfgKeyNext)
	cfgCurr := shardcfg.FromString(cfgStr)
	cfgNewInPast := shardcfg.FromString(cfgStrNewInPast)

	if cfgNewInPast.Num == cfgCurr.Num {
		return 
	}

	// rerun the configuation update
	sck.configMigrationHelper(cfgCurr, cfgNewInPast)

	// change current cfg to the new version
	sck.IKVClerk.Put(sck.cfgKey, cfgStrNewInPast, cfgVersionCurr)

}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	cfgStr := cfg.String()
	sck.IKVClerk.Put(sck.cfgKey, cfgStr, 0)
	sck.IKVClerk.Put(sck.cfgKeyNext, cfgStr, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	cfgStr, cfgVersionCurr, _ := sck.IKVClerk.Get(sck.cfgKey)
	old := shardcfg.FromString(cfgStr)
	newCfgStr := new.String()

	// before changing config, put the next config in storage first
	cfgStrNext, cfgVersionNew, _ := sck.IKVClerk.Get(sck.cfgKeyNext)

	// check if other controller already put the next config, if so, return directly
	cfgNext := shardcfg.FromString(cfgStrNext)
	if old.Num >= new.Num || cfgNext.Num >= new.Num {
		return 
	}
	// if put is failed, return directly
	err := sck.IKVClerk.Put(sck.cfgKeyNext, newCfgStr, cfgVersionNew)
	if err != rpc.OK {
		if err != rpc.ErrMaybe {
			return 
		} else if c, v, _ := sck.IKVClerk.Get(sck.cfgKeyNext); c != newCfgStr || cfgVersionNew + 1 != v {
			return 
		} 
	}

	// update grps and shards
	sck.configMigrationHelper(old, new)

	// change current cfg to the new version
	sck.IKVClerk.Put(sck.cfgKey, newCfgStr, cfgVersionCurr)	
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, _, _ := sck.IKVClerk.Get(sck.cfgKey)
	return shardcfg.FromString(cfgStr)
}

func (sck *ShardCtrler) configMigrationHelper(old, new *shardcfg.ShardConfig) {
	var wg sync.WaitGroup
	for shardId := range new.Shards {
		if new.Shards[shardId] == old.Shards[shardId] {
			continue
		}
		wg.Add(1)
		go func(oldShardId, newShardId tester.Tgid) {
			defer wg.Done()
			newGrp, _ := new.Groups[newShardId]
			oldGrp, _ := old.Groups[oldShardId]
			
			// install new shards or move shards
			// first freeze the shard on the old group if it exists
			oldClerk := shardgrp.MakeClerk(sck.clnt, oldGrp)
			shardState, err := oldClerk.FreezeShard(shardcfg.Tshid(shardId), old.Num)
			// handle the case where the cluster holding the shard is unreachable
			for err == shardrpc.ErrNoResp {
				cfgCurr := sck.Query()
				if cfgCurr.Num >= new.Num {
					return 
				}
				shardState, err = oldClerk.FreezeShard(shardcfg.Tshid(shardId), old.Num)
			}
			// when the freezeshard rpc is outdated, no need to keep going
			if err == shardrpc.ErrOutdatedCfg {
				utils.DPrintf("Outdated freezeshard for shard %v and config num %v, return is %v\n", shardId, old.Num, err)
				return 
			}

			// install the shard state on the new group 
			newClerk := shardgrp.MakeClerk(sck.clnt, newGrp)
			err = newClerk.InstallShard(shardcfg.Tshid(shardId), shardState, new.Num)
			for err == shardrpc.ErrNoResp {
				cfgCurr := sck.Query()
				if cfgCurr.Num >= new.Num {
					return 
				}
				err = newClerk.InstallShard(shardcfg.Tshid(shardId), shardState, new.Num)
			}

			// delete the shard on the old group if it exists
			err = oldClerk.DeleteShard(shardcfg.Tshid(shardId), old.Num)
			for err == shardrpc.ErrNoResp {
				cfgCurr := sck.Query()
				if cfgCurr.Num >= new.Num {
					return 
				}
				err = oldClerk.DeleteShard(shardcfg.Tshid(shardId), old.Num)
			}
				
		}(old.Shards[shardId], new.Shards[shardId])
	}
	wg.Wait()
}