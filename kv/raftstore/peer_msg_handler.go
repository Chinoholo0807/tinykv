package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}
func getRequestKey(request *raft_cmdpb.Request) (key []byte){
	switch  request.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = request.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = request.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = request.Delete.Key
	}
	return
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady(){
		rd := d.RaftGroup.Ready()
		//log.Infof("get ready form RawNode:%+v",rd)
		// save to storage
		snapResult ,err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		if snapResult != nil{
			if!reflect.DeepEqual(snapResult.Region,snapResult.PrevRegion){
				r := snapResult.Region
				d.peerStorage.SetRegion(r)
				storeMeta := d.ctx.storeMeta
				storeMeta.Lock()
				storeMeta.regions[r.Id] = r
				storeMeta.regionRanges.Delete(&regionItem{snapResult.PrevRegion})
				storeMeta.regionRanges.ReplaceOrInsert(&regionItem{snapResult.Region})
				storeMeta.Unlock()
			}
		}
		// sending raft messages to other peers through the network.
		d.Send(d.ctx.trans,rd.Messages)
		// all the available entries for execution,committed but not applied entries
		if len(rd.CommittedEntries) >0{
			kvWB := &engine_util.WriteBatch{}
			for _, entry := range rd.CommittedEntries{
				// apply the committed entries
				kvWB = d.processEntry(&entry,kvWB)
				if d.stopped{
					return
				}
			}
			// update apply state

			// apply success , update applyState.AppliedIndex
			d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId),d.peerStorage.applyState)
			// write back to DB
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)

		}
		// finish propose rd
		d.RaftGroup.Advance(rd)
	}
}
// processEntry process with given entry,try to apply the entry (entry.Data = raft_cmdpb.RaftCmdRequest)
func (d *peerMsgHandler) processEntry(entry *eraftpb.Entry,wb * engine_util.WriteBatch) *engine_util.WriteBatch{
	if entry.EntryType == eraftpb.EntryType_EntryConfChange{
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil{
			log.Panic(err)
		}
		d.processConfChange(cc,wb,entry)
		return wb
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil{
		log.Panic(err)
	}
	if len(msg.Requests)>0{
		return d.processRaftRequest(msg,wb,entry)
	}
	if msg.AdminRequest!=nil{
		d.processRaftAdminRequest(msg,wb,entry)
		return wb
	}
	return wb
}
func (d *peerMsgHandler) processConfChange(cc *eraftpb.ConfChange,wb *engine_util.WriteBatch,entry *eraftpb.Entry){
	msg:= &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err!= nil{
		log.Panic(err)
	}
	//  When a Region adds or removes Peer or splits, the Region’s epoch has changed.
	//  RegionEpoch’s conf_ver increases during ConfChange
	err = util.CheckRegionEpoch(msg,d.Region(),true)
	if err != nil{
			d.handleProposal(entry,func(p *proposal){
				p.cb.Done(ErrResp(err))
			})
		return
	}
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		region := d.Region()
		n := d.searchPeers(cc.NodeId,region.Peers)
		// do not exist
		if n == len(region.Peers){
			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers,peer)
			region.RegionEpoch.ConfVer ++
			log.Infof("%s apply AddNode req.peer:%d region's Peers:%s",
				d.Tag,peer.Id,engine_util.PeersString(region.Peers))
			meta.WriteRegionState(wb,region,rspb.PeerState_Normal)
			// update the region state in storeMeta of GlobalContext
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		// remove itself
		if d.Meta.Id == cc.NodeId{
			// call the destroyPeer() explicitly to stop the Raft module
			d.destroyPeer()
			return
		}
		// remove other peer with given regionID
		region := d.Region()
		n := d.searchPeers(cc.NodeId,region.Peers)
		// exist
		if n != len(region.Peers){
			region :=d.Region()
			//newPeers := make([]*metapb.Peer,0,len(region.Peers)-1)
			//for _, peer :=range region.Peers{
			//	// remove
			//	if peer.Id!= cc.NodeId{
			//		newPeers = append(newPeers,peer)
			//	}
			//}
			//region.Peers = newPeers
			region.Peers = append(region.Peers[:n], region.Peers[n+1:]...)
			region.RegionEpoch.ConfVer ++
			log.Infof("%s apply RemoteNode req.peer:%d region's Peers:%s",
				d.Tag,cc.NodeId,engine_util.PeersString(region.Peers))
			meta.WriteRegionState(wb,region,rspb.PeerState_Normal)
			// update the region state in storeMeta of GlobalContext
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.removePeerCache(cc.NodeId)
		}
	}
	d.RaftGroup.ApplyConfChange(*cc)
	d.handleProposal(entry,func( p * proposal){
		resp := newCmdResp()
		resp.AdminResponse= &raft_cmdpb.AdminResponse{
			CmdType : raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: d.Region(),
			},
		}
		p.cb.Done(resp)
	})
	if d.IsLeader(){
		//TODO
	}

}
func (d *peerMsgHandler) searchPeers(peerId uint64,peers []*metapb.Peer) int{
	for i,peer := range peers{
		if peer.Id == peerId{
			return i
		}
	}
	return len(peers)
}
// process raft_cmdpb.RaftCmdRequest.AdminRequest from Ready
func (d *peerMsgHandler) processRaftAdminRequest(request *raft_cmdpb.RaftCmdRequest,wb *engine_util.WriteBatch,entry *eraftpb.Entry){
	req := request.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactReq:=req.CompactLog
		applySt := d.peerStorage.applyState
		//log.Infof("processRaftAdminRequest:AdminCmdType_CompactLog " +
		//	"compactIdx:%d compactTerm:%d applySt.AppliedIndex:%d",compactReq.CompactIndex,
		//	compactReq.CompactTerm,applySt.AppliedIndex)
		// |---------lastTruncate------compacted--------| -->
		// |---------------------------lastTruncate-----|
		if compactReq.CompactIndex >= applySt.TruncatedState.Index{
			applySt.TruncatedState.Index = compactReq.CompactIndex
			applySt.TruncatedState.Term = compactReq.CompactTerm
			wb.SetMeta(meta.ApplyStateKey(d.regionId),applySt)
			d.ScheduleCompactLog(compactReq.CompactIndex)
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// do nothing,TransferLeader actually is an action with no need to replicate to other peers
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// be process by processEntry()
	case raft_cmdpb.AdminCmdType_Split:
		region := d.Region()
		err := util.CheckRegionEpoch(request,region,true)
		if err != nil{
			d.handleProposal(entry,func(p *proposal){
				p.cb.Done(ErrResp(err))
			})
			return
		}
		split := req.GetSplit()
		err = util.CheckKeyInRegion(split.SplitKey,region)
		if err!= nil{
			d.handleProposal(entry,func(p *proposal){
				p.cb.Done(ErrResp(err))
			})
			return
		}
		//logs.Infof("%s handle SplitCmd splitReq:%+v",d.Tag,split)
		// update storeMeta
		storeMeta :=d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{region})
		// version increases during a split
		region.RegionEpoch.Version ++
		newPeers := make([]*metapb.Peer,0)
		// new region's peers is as same as region
		for i,peer := range region.Peers{
			newPeers  = append(newPeers,&metapb.Peer{
				Id:split.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
		}
		// newRegion : [splitKey,endKey)
		// oldRegion : [startKey,splitKey)
		newRegion := &metapb.Region{
			Id:split.NewRegionId,
			StartKey: split.SplitKey,
			EndKey: region.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: newPeers,
		}
		region.EndKey = split.SplitKey
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{newRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region})
		storeMeta.Unlock()
		// update region's meta info
		meta.WriteRegionState(wb,region,rspb.PeerState_Normal)
		meta.WriteRegionState(wb,newRegion,rspb.PeerState_Normal)
		d.SizeDiffHint = 0
		// reset
		d.ApproximateSize = new(uint64)
		// create the new peer & register it
		peer,err := createPeer(d.storeID(),d.ctx.cfg,d.ctx.regionTaskSender,d.ctx.engine,newRegion)
		if err != nil{
			log.Panic(err)
		}
		d.ctx.router.register(peer)
		// start the new peer
		d.ctx.router.send(newRegion.Id,message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
		d.handleProposal(entry,func(p * proposal){
			resp := newCmdResp()
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					// Regions contain the region where the specific key have split into
					Regions: []*metapb.Region{region,newRegion},
				},
			}
			p.cb.Done(resp)
		})
		if d.IsLeader(){
			// TODO
		}
	}
}
// process raft_cmdpb.RaftCmdRequest.Requests from Ready
func (d *peerMsgHandler) processRaftRequest(msg *raft_cmdpb.RaftCmdRequest,wb *engine_util.WriteBatch,entry * eraftpb.Entry) *engine_util.WriteBatch{
	req := msg.Requests[0]
	key := getRequestKey(req)
	if key != nil{

	}
	// apply to kv
	switch  req.CmdType {
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Put:
		wb.SetCF(req.Put.Cf,key,req.Put.Value)
	case raft_cmdpb.CmdType_Delete:
		wb.DeleteCF(req.Delete.Cf,key)
	case raft_cmdpb.CmdType_Snap:
	}
	d.handleProposal(entry, func (p * proposal){
		resp := newCmdResp()
		// build the resp of cmd
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId),d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)
			value,err := engine_util.GetCF(d.peerStorage.Engines.Kv,req.Get.GetCf(),key)
			if err != nil{
				value = nil
			}
			resp.Responses = []*raft_cmdpb.Response{{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{Value: value},
			}}
			wb = new(engine_util.WriteBatch)
		case raft_cmdpb.CmdType_Put:
			resp.Responses = []*raft_cmdpb.Response{{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:&raft_cmdpb.PutResponse{},
			}}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteResponse{},
			}}
		case raft_cmdpb.CmdType_Snap:
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version{
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId),d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)

			resp.Responses = []*raft_cmdpb.Response{{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			}}
			// used for snap
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			wb = new(engine_util.WriteBatch)
		}
		p.cb.Done(resp)
	})
	return wb
}
func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry,handle func ( *proposal)){
	if len(d.proposals) > 0{
		p := d.proposals[0]
		if p.index == entry.Index{
			if p.term == entry.Term{
				handle(p)
			}else{
				NotifyStaleReq(entry.Term,p.cb)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

// HandleMsg processes all the messages received from raftCh
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		// redirect
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// propose raft_cmdpb.RaftCmdRequest
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// Your Code Here (2B).
	if msg.AdminRequest != nil{
		d.proposeRaftAdminRequest(msg,cb)
	} else{
		d.proposeRaftRequest(msg,cb)
	}

}
// propose raft_cmdpb.RaftCmdRequest.Requests from raftCh
func (d * peerMsgHandler) proposeRaftRequest(msg *raft_cmdpb.RaftCmdRequest,cb * message.Callback){
	if len(msg.Requests) == 0 {
		return
	}
	key := getRequestKey(msg.Requests[0])
	if key != nil{
		err :=util.CheckKeyInRegion(key,d.Region())
		if err != nil{
			cb.Done(ErrResp(err))
			return
		}
	}
	data ,err := msg.Marshal()
	if err!= nil{
		log.Panic(err)
	}

	p := &proposal{
		index: d.nextProposalIndex(),
		term: d.Term(),
		cb: cb,
	}
	d.proposals = append(d.proposals,p)
	// propose data to be appended to the raft log
	d.RaftGroup.Propose(data)
}
// propose raft_cmdpb.RaftCmdRequest.AdminRequest from raftCh
func (d * peerMsgHandler) proposeRaftAdminRequest(msg *raft_cmdpb.RaftCmdRequest,cb * message.Callback){
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data,err := msg.Marshal()
		if err != nil{
			panic(err)
		}
		// propose data to be appended to the raft log
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_Split:
		// propose the split cmd
		err := util.CheckKeyInRegion(req.Split.SplitKey,d.Region())
		if err != nil{
			cb.Done(ErrResp(err))
			return
		}
		data,err := msg.Marshal()
		if err != nil{
			log.Panic(err)
		}
		p := &proposal{
			index: d.nextProposalIndex(),
			term: d.Term(),
			cb: cb,
		}
		d.proposals = append(d.proposals,p)
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// some config change do not apply
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex(){
			return
		}
		context,err:=msg.Marshal()
		if err!= nil{
			log.Panic(err)
		}
		cc := eraftpb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId: req.ChangePeer.Peer.Id,
			Context: context,
		}
		p := &proposal{
			index: d.nextProposalIndex(),
			term: d.Term(),
			cb: cb,
		}
		d.proposals = append(d.proposals,p)
		d.RaftGroup.ProposeConfChange(cc)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// TransferLeader actually is an action with no need to replicate to other peers
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:raft_cmdpb.AdminCmdType_TransferLeader,
		}
		cb.Done(resp)
	}
}
func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

// ScheduleCompactLog send a task to gc log in [lastCompactedIdx, truncatedIndex] with given region
func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	//log.Infof("raftLogGCTask:%+v",*raftLogGCTask)
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	// stop current peerMsgHandler to avoid process suffix entries
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
