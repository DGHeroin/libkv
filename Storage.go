package libkv

import (
    "crypto/tls"
    "errors"
    "time"
)

type Storage interface {
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

type WriteOptions struct {
    TTL time.Duration
}

type Config struct {
    ClientTLS         *ClientTLSConfig
    TLS               *tls.Config
    ConnectionTimeout time.Duration
    Bucket            string
    Username          string
    Password          string
    DB                int
}

func DefaultConfig() *Config {
    return &Config{
        ClientTLS:         nil,
        TLS:               nil,
        ConnectionTimeout: time.Second * 5,
        Bucket:            "",
        Username:          "",
        Password:          "",
        DB:                0,
    }
}
type ClientTLSConfig struct {
    CertFile   string
    KeyFile    string
    CACertFile string
}

type KVPair struct {
    Key       string
    Value     []byte
    LastIndex uint64
}
type LockOptions struct {
    Value     []byte        // Optional, value to associate with the lock
    TTL       time.Duration // Optional, expiration ttl associated with the lock
    RenewLock chan struct{} // Optional, chan used to control and stop the session ttl renewal for the lock
}

type Locker interface {
    Lock(stopChan chan struct{}) (<-chan struct{}, error)
    Unlock() error
}

var (
    ErrStorageNotSupport = errors.New("storage not supported yet")
)
