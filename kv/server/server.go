package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader ,err := server.storage.Reader(req.Context)
	if err != nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader,req.Version)
	lock,err := txn.GetLock(req.GetKey())
	if err!=nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	// If the key to be read is locked by another transaction
	// TinyKV should return an error.
	if lock != nil && req.Version >= lock.Ts{
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.GetKey()),
		}
		return resp,nil
	}
	// Otherwise, TinyKV must search the versions of the key to find the most recent, valid value.
	value ,err := txn.GetValue(req.GetKey())
	if err !=nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	if value == nil{
		resp.NotFound = true
		return resp ,nil
	}
	resp.Value = value
	return resp,nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp:= &kvrpcpb.PrewriteResponse{}
	reader ,err := server.storage.Reader(req.Context)
	if err!=nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader,req.StartVersion)
	errors := make([]*kvrpcpb.KeyError,0)
	// for specific key，try to prewrite value & lock key
	for _, mutation:=range req.Mutations{
		write,commitTs,err:=txn.MostRecentWrite(mutation.Key)
		if err!=nil{
			if regionErr,ok := err.(*raft_storage.RegionError);ok{
				resp.RegionError = regionErr.RequestErr
				return resp,nil
			}
			return nil,err
		}
		// write-write conflict
		if write!=nil && txn.StartTS <= commitTs{
			conflictErr:=&kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: req.StartVersion,
					ConflictTs: commitTs,
					Key: mutation.Key,
					Primary: req.PrimaryLock,
				},
			}
			errors = append(errors,conflictErr)
			continue
		}
		// try to get the value's lock
		lock,err:=txn.GetLock(mutation.Key)
		if err!=nil{
			if regionErr,ok := err.(*raft_storage.RegionError);ok{
				resp.RegionError = regionErr.RequestErr
				return resp,nil
			}
			return nil,err
		}
		// the key is locked by other txn
		if lock != nil && lock.Ts != req.StartVersion{
			lockErr := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key: mutation.Key,
					LockTtl: lock.Ttl,
				},
			}
			errors = append(errors,lockErr)
			continue
		}
		// lock the key & store the value
		switch mutation.Op {
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key,mutation.Value)
		case kvrpcpb.Op_Lock:
		case kvrpcpb.Op_Rollback:
		}
		writeKind := mvcc.WriteKindFromProto(mutation.Op)
		txn.PutLock(mutation.Key,&mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:txn.StartTS,
			Ttl: req.LockTtl,
			Kind: writeKind,
		})
	}
	if len(errors) > 0{
		resp.Errors  =errors
		return resp,nil
	}
	err = server.storage.Write(req.Context,txn.Writes())
	if err!=nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	return resp ,nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader ,err := server.storage.Reader(req.Context)
	if err!=nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader,req.StartVersion)
	// try to commit primary record
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	// once the primary key commit success ,the txn commit success
	for _,key := range req.Keys{
		lock,err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// lock by this txn do not exist
		if lock == nil{
			return resp,nil
		}
		if lock.Ts != req.StartVersion{
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp,nil
		}
		// put write
		txn.PutWrite(key,req.CommitVersion,&mvcc.Write{
			StartTS: txn.StartTS,
			Kind: lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context,txn.Writes())
	if err!=nil{
		if regionErr,ok := err.(*raft_storage.RegionError);ok{
			resp.RegionError = regionErr.RequestErr
			return resp,nil
		}
		return nil,err
	}
	return resp,nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
