package etcdv3

import (
    "context"
    "github.com/DGHeroin/libkv"
    v3 "go.etcd.io/etcd/clientv3"
    "time"
)

func init() {
    libkv.AddStorage("etcdv3", New)
}
func New(addrs []string, opt *libkv.Config) (libkv.Storage, error) {
    if opt == nil {
        opt = libkv.DefaultConfig()
    }
    v := &etcdv3Impl{
        addrs:   addrs,
        opt:     opt,
        timeout: opt.ConnectionTimeout,
    }
    client, err := v3.New(v3.Config{
        Endpoints:   addrs,
        DialTimeout: time.Second * 5,
        Username:    opt.Username,
        Password:    opt.Password,
        TLS:         opt.TLS,
    })
    if err != nil {
        return nil, err
    }
    v.client = client
    return v, nil
}

type etcdv3Impl struct {
    addrs   []string
    opt     *libkv.Config
    client  *v3.Client
    timeout time.Duration
    done    chan struct{}
}

func (s *etcdv3Impl) Put(key string, value []byte, options *libkv.WriteOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    _, err := s.client.Put(ctx, key, string(value))

    return err
}

func (s *etcdv3Impl) Get(key string) (*libkv.KVPair, error) {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    resp, err := s.client.Get(ctx, key)
    if err != nil {
        return nil, err
    }
    r := &libkv.KVPair{
        Key: key,
    }
    for _, ev := range resp.Kvs {
        r.Value = ev.Value
        break
    }
    return r, nil
}

func (s *etcdv3Impl) Delete(key string) error {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    _, err := s.client.Delete(ctx, key)
    return err
}

func (s *etcdv3Impl) Exists(key string) (bool, error) {
    r, err := s.Get(key)
    if err != nil {
        return false, err
    }
    return r.Value != nil, nil
}

func (s *etcdv3Impl) Watch(key string, stopCh <-chan struct{}) (<-chan *libkv.KVPair, error) {
    watchCh := make(chan *libkv.KVPair)
    go func() {
        defer close(watchCh)
        pair, err := s.Get(key)
        if err != nil {
            return
        }
        watchCh <- pair
        rch := s.client.Watch(context.Background(), key)
        for {
            select {
            case <- stopCh:
                return
            case <-s.done:
                return
            case wresp := <-rch:
                for _, event := range wresp.Events {
                    watchCh <- &libkv.KVPair{
                        Key:       string(event.Kv.Key),
                        Value:     event.Kv.Value,
                        LastIndex: uint64(event.Kv.Version),
                    }
                }
            }
        }
    }()
    return watchCh, nil
}

func (s *etcdv3Impl) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*libkv.KVPair, error) {
    watchCh := make(chan []*libkv.KVPair)
    go func() {
        defer close(watchCh)

        list, err := s.List(dir)
        if err != nil {
            return
        }
        watchCh <- list
        rch := s.client.Watch(context.Background(), dir, v3.WithPrefix())
        for {
            select {
            case <-s.done:
                return
            case <-rch:
                list, err := s.List(dir)
                if err != nil {
                    return
                }
                watchCh <- list
            }
        }
    }()
    return watchCh, nil
}

func (s *etcdv3Impl) NewLock(key string, options *libkv.LockOptions) (libkv.Locker, error) {
    panic("implement me")
}

func (s *etcdv3Impl) List(dir string) ([]*libkv.KVPair, error) {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    resp, err := s.client.Get(ctx, dir, v3.WithPrefix())
    if err != nil {
        return nil, err
    }

    kvs := make([]*libkv.KVPair, 0, len(resp.Kvs))
    for _, kv := range resp.Kvs {
        kvs = append(kvs, &libkv.KVPair{
            Key:       string(kv.Key),
            Value:     kv.Value,
            LastIndex: uint64(kv.Version),
        })
    }
    return kvs, nil
}

func (s *etcdv3Impl) DeleteTree(dir string) error {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    _, err := s.client.Delete(ctx, dir, v3.WithPrefix())
    return err
}

func (s *etcdv3Impl) AtomicPut(key string, value []byte, previous *libkv.KVPair, options *libkv.WriteOptions) (bool, *libkv.KVPair, error) {
    panic("implement me")
}

func (s *etcdv3Impl) AtomicDelete(key string, previous *libkv.KVPair) (bool, error) {
    panic("implement me")
}

func (s *etcdv3Impl) Close() {
    s.client.Close()
    close(s.done)
}
