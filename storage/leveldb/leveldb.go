package leveldb

import (
    "errors"
    "github.com/DGHeroin/libkv"
    "github.com/DGHeroin/libkv/storage"
    ldb "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/util"
)

func init() {
    libkv.AddStorage("leveldb", New)
}
func New(addrs []string, opt *storage.Config) (storage.Storage, error) {
    if opt == nil {
        opt = &storage.Config{
        }
    }
    if len(addrs) == 0 {
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

func (s *leveldbImpl) Put(key string, value []byte, options *storage.WriteOptions) error {
    return s.db.Put([]byte(key), value, nil)
}

func (s *leveldbImpl) Get(key string) (*storage.KVPair, error) {
    val, err := s.db.Get([]byte(key), nil)
    if err != nil {
        return nil, err
    }
    return &storage.KVPair{
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

func (s *leveldbImpl) Watch(key string, stopCh <-chan struct{}) (<-chan *storage.KVPair, error) {
    panic("implement me")
}

func (s *leveldbImpl) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*storage.KVPair, error) {
    panic("implement me")
}

func (s *leveldbImpl) NewLock(key string, options *storage.LockOptions) (storage.Locker, error) {
    panic("implement me")
}

func (s *leveldbImpl) List(dir string) ([]*storage.KVPair, error) {
    iter := s.db.NewIterator(util.BytesPrefix([]byte(dir)), nil)
    var result = make([]*storage.KVPair, 16)
    for iter.Next() {
        result = append(result, &storage.KVPair{
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

func (s *leveldbImpl) AtomicPut(key string, value []byte, previous *storage.KVPair, options *storage.WriteOptions) (bool, *storage.KVPair, error) {
    panic("implement me")
}

func (s *leveldbImpl) AtomicDelete(key string, previous *storage.KVPair) (bool, error) {
    panic("implement me")
}

func (s *leveldbImpl) Close() {
    s.db.Close()
}
