package storage

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

type WriteOptions struct{}

type Config struct {
	ClientTLS         *ClientTLSConfig
	TLS               *tls.Config
	ConnectionTimeout time.Duration
	Bucket            string
	Username          string
	Password          string
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
}

type Locker struct{}

var (
	ErrStorageNotSupport = errors.New("storage not supported yet")
)
