package libkv

type Store interface {
    Put(key string, value []byte, options *WriteOptions) error
    Get(key string) (*KVPair, error)
    Delete(key string) error
    Exists(key string) (bool, error)
    Watch(key string, stopCh <-chan struct{}) (<-chan *KVPair, error)
    WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*KVPair, error)
    NewLock(key string, options *LockOptions) (Locker, error)
    List(dir string) ([]*KVPair, error)
    DeleteTree(dir string) error
    AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error)
    AtomicDelete(key string, previous *KVPair) (bool, error)
    Close()
}