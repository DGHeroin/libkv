package redis

import (
    "context"
    "fmt"
    "github.com/DGHeroin/libkv"
    "github.com/DGHeroin/libkv/common"
    rdb "github.com/go-redis/redis/v8"
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
    err := r.client.Set(ctx, key, value, expiration).Err()
    if err != nil {
        return err
    }
    return r.client.Publish(ctx, key, value).Err()
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
    fn = func(tx *rdb.Tx) error {
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
    return r.WatchMulti(stopCh, key)
}

//
func (r *redisImpl) WatchMulti(stopCh <-chan struct{}, keys ...string) (<-chan *libkv.KVPair, error) {
    watchCh := make(chan *libkv.KVPair)
    go func() {
        defer close(watchCh)
        for _, key := range keys {
            pair, err := r.Get(key)
            if err != nil {
                continue
            }
            watchCh <- pair
        }
        rch := r.client.PSubscribe(context.Background(), keys...)
        for {
            select {
            case <-stopCh:
                return
            case evt := <-rch.Channel():
                if evt != nil {
                    watchCh <- &libkv.KVPair{
                        Key:       evt.Channel,
                        Value:     []byte(evt.Payload),
                        LastIndex: 0,
                    }
                }
            }
        }
    }()
    return watchCh, nil
}
func (r *redisImpl) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*libkv.KVPair, error) {
    watchCh := make(chan []*libkv.KVPair)
    go func() {
        defer close(watchCh)

        list, err := r.List(dir)
        if err != nil {
            return
        }
        watchCh <- list
        rch := r.client.PSubscribe(context.Background(), dir+"*")
        for {
            select {
            case <-stopCh:
                return
            case evt := <-rch.Channel():
                watchCh <- []*libkv.KVPair{
                    {
                        Key:       evt.Channel,
                        Value:     []byte(evt.Payload),
                        LastIndex: 0,
                    },
                }
            }
        }
    }()
    return watchCh, nil
}

func (r *redisImpl) NewLock(key string, options *libkv.LockOptions) (libkv.Locker, error) {
    panic("implement me")
}

func (r *redisImpl) List(dir string) ([]*libkv.KVPair, error) {
    ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
    defer cancel()

    var (
        cursor = uint64(0)
        n      = int(0)
        result []*libkv.KVPair
    )
    k := fmt.Sprintf("%s*", dir)

    for {
        var (
            keys []string
            err  error
        )
        keys, cursor, err = r.client.Scan(ctx, cursor, k, 20).Result()
        if err != nil {
            return result, err
        }
        n += len(keys)
        for _, key := range keys {
            data, err2 := r.client.Get(ctx, key).Bytes()
            if err2 != nil {
                continue
            }
            result = append(result, &libkv.KVPair{
                Key:       key,
                Value:     data,
                LastIndex: 0,
            })
        }
        if cursor == 0 {
            break
        }
    }
    return result, nil
}

func (r *redisImpl) DeleteTree(dir string) error {
    return r.client.Del(context.Background(), dir).Err()
}

func (r *redisImpl) AtomicPut(key string, value []byte, previous *libkv.KVPair, options *libkv.WriteOptions) (bool, *libkv.KVPair, error) {
    return false, nil, common.ErrAPINotSupported
}

func (r *redisImpl) AtomicDelete(key string, previous *libkv.KVPair) (bool, error) {
    return false, common.ErrAPINotSupported
}

func (r *redisImpl) Close() {
    _ = r.client.Close()
}
