// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}
// use for sort
type StoreSlice []*core.StoreInfo
func (ss StoreSlice) Len() int{ return len(ss)}
func (ss StoreSlice) Swap(i,j int) { ss[i],ss[j] = ss[j],ss[i]}
func (ss StoreSlice) Less(i,j int) bool{ return ss[i].GetRegionSize() < ss[j].GetRegionSize()}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// First, the Scheduler will select all suitable stores.
	// a suitable store should be up and the down time cannot be longer than MaxStoreDownTime of the cluster
	stores := make(StoreSlice,0)
	for _, store := range cluster.GetStores(){
		// the store is alive
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime(){
			stores = append(stores,store)
		}
	}
	n := stores.Len()
	if n < 2{
		return nil
	}
	// Then sort them according to their region size.
	// Then the Scheduler tries to find regions to move from the store with the biggest region size.
	sort.Sort(stores)
	i:=n-1
	var region *core.RegionInfo
	for ;i>=0;i--{
		//The scheduler will try to find the region most suitable for moving in the store.
		// First, it will try to select a pending region because pending may mean the disk is overloaded.
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(),func(rc core.RegionsContainer){
				regions = rc
		})
		region = regions.RandomRegion(nil,nil)
		if region != nil {
			break
		}
		// If there isn’t a pending region, it will try to find a follower region.
		cluster.GetFollowersWithLock(stores[i].GetID(),func(rc core.RegionsContainer){
			regions = rc
		})
		region = regions.RandomRegion(nil,nil)
		if region !=nil{
			break
		}
		// If it still cannot pick out one region, it will try to pick leader regions
		cluster.GetLeadersWithLock(stores[i].GetID(),func(rc core.RegionsContainer){
			regions = rc
		})
		region = regions.RandomRegion(nil,nil)
		if region != nil{
			break
		}
		// Finally, it will select out the region to move,
		// or the Scheduler will try the next store which has a smaller region size until all stores will have been tried.
	}
	if region == nil{
		// no region was chosen,skip
		return nil
	}
	// After you pick up one region to move, the Scheduler will select a store as the target
	// the Scheduler will select the store with the smallest region size.
	srcStore := stores[i]
	var dstStore *core.StoreInfo
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas(){
		return nil
	}
	for j:=0;j<i;j++{
		if _,ok:=storeIds[stores[j].GetID()];!ok{
			dstStore = stores[j]
			break
		}
	}
	if dstStore == nil{
		return nil
	}
	// judge whether this movement is valuable,
	// by checking the difference between region sizes of the original store and the target store.
	if diff := srcStore.GetRegionSize() - dstStore.GetRegionSize();diff <= 2*region.GetApproximateSize(){
		return nil
	}
	newPeer ,err := cluster.AllocPeer(dstStore.GetID())
	if err!=nil{
		log.Errorf("cluster.AllocPeer failed,err:%+v",err)
		return nil
	}
	desc := fmt.Sprintf("move from src-store-id(%d) to dst-store-id(%d) with region(%d)",srcStore.GetID(),dstStore.GetID(),region.GetID())

	op, err := operator.CreateMovePeerOperator(desc,cluster,region,operator.OpBalance,srcStore.GetID(),dstStore.GetID(),newPeer.GetId())
	if err!=nil{
		log.Errorf("opeartor.CreateMovePeerOperator failed,err:%+v",err)
		return nil
	}
	return op
}
