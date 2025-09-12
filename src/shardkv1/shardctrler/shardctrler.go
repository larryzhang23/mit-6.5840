package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"

	"6.5840/kvsrv1"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
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
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.cfgKey = "cfg"
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	cfgStr := cfg.String()
	utils.DPrintf("init config: %v\n", cfgStr)
	sck.IKVClerk.Put(sck.cfgKey, cfgStr, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	utils.DPrintf("new config: %v\n", new)
	cfgStr, cfgVersion, _ := sck.IKVClerk.Get(sck.cfgKey)
	old := shardcfg.FromString(cfgStr)

	sck.configMigrationHelper(old, new)

	newCfgStr := new.String()
	sck.IKVClerk.Put(sck.cfgKey, newCfgStr, cfgVersion)
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
			newGrp, newOk := new.Groups[newShardId]
			oldGrp, oldOk := old.Groups[oldShardId]
			if !newOk || !oldOk {
				log.Fatalf("new shardgrp or old shardgrp does not exists (newOk %v, oldOk %v)\n", newOk, oldOk)
			}
			// install new shards or move shards
			// first freeze the shard on the old group if it exists
			oldClerk := shardgrp.MakeClerk(sck.clnt, oldGrp)
			shardState, _ := oldClerk.FreezeShard(shardcfg.Tshid(shardId), old.Num)

			// install the shard state on the new group 
			newClerk := shardgrp.MakeClerk(sck.clnt, newGrp)
			newClerk.InstallShard(shardcfg.Tshid(shardId), shardState, new.Num)

			// delete the shard on the old group if it exists
			oldClerk.DeleteShard(shardcfg.Tshid(shardId), old.Num)
				
		}(old.Shards[shardId], new.Shards[shardId])
	}
	wg.Wait()
}