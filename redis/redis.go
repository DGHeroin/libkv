package redis

import (
    "context"
    "github.com/DGHeroin/libkv"
    rdb "github.com/go-redis/redis/v8"
    "log"
    "time"
)

func init() {
    libkv.AddStorage("redis", New)
}
func New(endpoints []string, opt *libkv.Config) (libkv.Storage, error) {
    if opt == nil {
        opt = libkv.DefaultConfig()
    }
    r := &redisImpl{
        timeout: opt.ConnectionTimeout,
    }
    cli := rdb.NewClient(&rdb.Options{
        Network:   "",
        Addr:      endpoints[0],
        Username:  opt.Username,
        Password:  opt.Username,
        DB:        opt.DB,
        TLSConfig: opt.TLS,
    })
    r.client = cli
    err := cli.Ping(context.Background()).Err()
    return r, err
}

type redisImpl struct {
    client  *rdb.Client
    timeout time.Duration
}

func (r *redisImpl) Put(key string, value []byte, options *libkv.WriteOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
    defer cancel()
    expiration := time.Duration(0)
    if options != nil {
        expiration = options.TTL
    }
    log.Println("put", key, string(value))
    return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *redisImpl) Get(key string) (*libkv.KVPair, error) {
    ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
    defer cancel()
    cmd := r.client.Get(ctx, key)
    if cmd.Err() != nil {
        return nil, cmd.Err()
    }
    data, err := cmd.Bytes()
    return &libkv.KVPair{
        Key:       key,
        Value:     data,
        LastIndex: 0,
    }, err
}

func (r *redisImpl) Delete(key string) error {
    ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
    defer cancel()
    return r.client.Del(ctx, key).Err()
}

func (r *redisImpl) Exists(key string) (bool, error) {
    ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
    defer cancel()
    cmd := r.client.Exists(ctx, key)
    if cmd.Err() != nil {
        return false, cmd.Err()
    }
    return cmd.Val() == 1, nil
}

func (r *redisImpl) watch(key string, watchCh chan *libkv.KVPair) func(tx *rdb.Tx) error {
    var fn func(tx *rdb.Tx) error
    fn= func(tx *rdb.Tx) error {
        v, err := tx.Get(context.Background(), key).Bytes()
        
        if err != nil && err != rdb.Nil {
            return err
        }
        watchCh <- &libkv.KVPair{
            Key:       key,
            Value:     v,
            LastIndex: 0,
        }
        return nil
    }
    return fn
}
func (r *redisImpl) Watch(key string, stopCh <-chan struct{}) (<-chan *libkv.KVPair, error) {
    watchCh := make(chan *libkv.KVPair, 1)
    go r.client.Watch(context.Background(), r.watch(key, watchCh), []string{key}...)
    
    return watchCh, nil
}

func (r *redisImpl) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*libkv.KVPair, error) {
    panic("implement me")
}

func (r *redisImpl) NewLock(key string, options *libkv.LockOptions) (libkv.Locker, error) {
    panic("implement me")
}

func (r *redisImpl) List(dir string) ([]*libkv.KVPair, error) {
    panic("implement me")
}

func (r *redisImpl) DeleteTree(dir string) error {
    panic("implement me")
}

func (r *redisImpl) AtomicPut(key string, value []byte, previous *libkv.KVPair, options *libkv.WriteOptions) (bool, *libkv.KVPair, error) {
    panic("implement me")
}

func (r *redisImpl) AtomicDelete(key string, previous *libkv.KVPair) (bool, error) {
    panic("implement me")
}

func (r *redisImpl) Close() {
    r.client.Close()
}
