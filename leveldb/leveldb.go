package leveldb

import (
	"errors"
	"github.com/DGHeroin/libkv"
	"github.com/DGHeroin/libkv/kv"
	ldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func init() {
	kv.NewMap["leveldb"] = New
}
func New(addrs []string, opt *libkv.Options) (libkv.Store, error) {
	if opt == nil {
		opt = &libkv.Options{
		}
	}
	if len(addrs) == 0{
		return nil, errors.New("leveldb path unspecified")
	}
	v := &leveldbImpl{
		path: addrs[0],
	}
	db, err := ldb.OpenFile(v.path, nil)
	if err != nil {
		return nil, err
	}
	v.db = db
	return v, nil
}

type leveldbImpl struct {
	path string
	db   *ldb.DB
}

func (s *leveldbImpl) Put(key string, value []byte, options *libkv.WriteOptions) error {
	return s.db.Put([]byte(key), value, nil)
}

func (s *leveldbImpl) Get(key string) (*libkv.KVPair, error) {
	val, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	return &libkv.KVPair{
		Key:       key,
		Value:     val,
		LastIndex: 0,
	}, nil
}

func (s *leveldbImpl) Delete(key string) error {
	return s.db.Delete([]byte(key), nil)
}

func (s *leveldbImpl) Exists(key string) (bool, error) {
	return s.db.Has([]byte(key), nil)
}

func (s *leveldbImpl) Watch(key string, stopCh <-chan struct{}) (<-chan *libkv.KVPair, error) {
	panic("implement me")
}

func (s *leveldbImpl) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*libkv.KVPair, error) {
	panic("implement me")
}

func (s *leveldbImpl) NewLock(key string, options *libkv.LockOptions) (libkv.Locker, error) {
	panic("implement me")
}

func (s *leveldbImpl) List(dir string) ([]*libkv.KVPair, error) {
	iter := s.db.NewIterator(util.BytesPrefix([]byte(dir)), nil)
	var result = make([]*libkv.KVPair, 16)
	for iter.Next() {
		result = append(result, &libkv.KVPair{
			Key:       string(iter.Key()),
			Value:     iter.Value(),
			LastIndex: 0,
		})
	}
	return result, nil
}

func (s *leveldbImpl) DeleteTree(dir string) error {
	list, err := s.List(dir)
	if err != nil {
		return err
	}
	batch := new(ldb.Batch)
	for _, val := range list {
		batch.Delete([]byte(val.Key))
	}
	return s.db.Write(batch, nil)
}

func (s *leveldbImpl) AtomicPut(key string, value []byte, previous *libkv.KVPair, options *libkv.WriteOptions) (bool, *libkv.KVPair, error) {
	panic("implement me")
}

func (s *leveldbImpl) AtomicDelete(key string, previous *libkv.KVPair) (bool, error) {
	panic("implement me")
}

func (s *leveldbImpl) Close() {
	s.db.Close()
}
