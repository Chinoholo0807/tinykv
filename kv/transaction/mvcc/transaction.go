package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/errors"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			// key ascend while ts descend
			Key: EncodeKey(key,ts),
			Value: write.ToBytes(),
			Cf:engine_util.CfWrite,
		},
	}
	txn.writes = append(txn.writes,modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value,err:=txn.Reader.GetCF(engine_util.CfLock,key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if value == nil{
		return nil,nil
	}
	lock ,err := ParseLock(value)
	if err != nil{
		return nil,errors.WithStack(err)
	}
	return lock ,nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key: key,
			Value: lock.ToBytes(),
			Cf:engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf: engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes,modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	// seek(keyB_timestamp2) would be keyB_timestamp1(if exist) or keyC_timestamp1
	iter.Seek(EncodeKey(key,txn.StartTS))
	if !iter.Valid(){
		return nil,nil
	}
	item := iter.Item()
	userKey:= DecodeUserKey(item.KeyCopy(nil))
	if bytes.Compare(userKey,key) != 0{
		return nil,nil
	}
	value,err := item.ValueCopy(nil)
	if err !=nil{
		return nil,errors.WithStack(err)
	}
	write ,err:= ParseWrite(value)
	if err != nil{
		return nil,errors.WithStack(err)
	}
	if write.Kind == WriteKindDelete{
		return nil,nil
	}
	return txn.Reader.GetCF(engine_util.CfDefault,EncodeKey(key,write.StartTS))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key: EncodeKey(key,txn.StartTS),
			Value: value,
			Cf:engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes,modify)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key:EncodeKey(key,txn.StartTS),
			Cf:engine_util.CfDefault,
		},
	}
	txn.writes =append(txn.writes,modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 获取对于指定的key的write{StartTs,Kind}和CommitTs，该Write的StartTs与txn的StartTs一致
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter :=txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	for iter.Seek(EncodeKey(key,TsMax));iter.Valid();iter.Next(){
		item := iter.Item()
		itemKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(itemKey)
		commitTs := decodeTimestamp(itemKey)
		if bytes.Compare(key,userKey) !=0{
			return nil,0,nil
		}
		itemValue,err := item.ValueCopy(nil)
		if err !=nil{
			return nil,0,errors.WithStack(err)
		}
		write,err := ParseWrite(itemValue)
		if err !=nil{
			return nil,0,errors.WithStack(err)
		}
		if write.StartTS == txn.StartTS{
			return write,commitTs,nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 获取到对于给定key的最新的一个write{StartTs,Kind}和对应的CommitTs
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key,TsMax))
	if !iter.Valid(){
		return nil,0,nil
	}
	item:= iter.Item()
	itemKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(itemKey)
	commitTs := decodeTimestamp(itemKey)
	if bytes.Compare(userKey,key)!=0{
		return nil,0,nil
	}
	itemValue,err := item.ValueCopy(nil)
	if err !=nil{
		return nil,0,errors.WithStack(err)
	}
	write,err := ParseWrite(itemValue)
	if err !=nil{
		return nil,0,errors.WithStack(err)
	}
	return write,commitTs,nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
