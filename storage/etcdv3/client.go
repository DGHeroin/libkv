package etcdv3

import (
    "context"
    "github.com/DGHeroin/libkv"
    "github.com/DGHeroin/libkv/storage"
    v3 "go.etcd.io/etcd/clientv3"
    "time"
)

func init() {
    libkv.AddStorage("etcdv3", New)
}
func New(addrs []string, opt *storage.Config) (storage.Storage, error) {
    if opt == nil {
        opt = &storage.Config{
            ConnectionTimeout: time.Second * 5,
        }
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
    opt     *storage.Config
    client  *v3.Client
    timeout time.Duration
    done    chan struct{}
}

func (s *etcdv3Impl) Put(key string, value []byte, options *storage.WriteOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    _, err := s.client.Put(ctx, key, string(value))

    return err
}

func (s *etcdv3Impl) Get(key string) (*storage.KVPair, error) {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    resp, err := s.client.Get(ctx, key)
    if err != nil {
        return nil, err
    }
    r := &storage.KVPair{
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

func (s *etcdv3Impl) Watch(key string, stopCh <-chan struct{}) (<-chan *storage.KVPair, error) {
    watchCh := make(chan *storage.KVPair)
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
                    watchCh <- &storage.KVPair{
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

func (s *etcdv3Impl) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*storage.KVPair, error) {
    watchCh := make(chan []*storage.KVPair)
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

func (s *etcdv3Impl) NewLock(key string, options *storage.LockOptions) (storage.Locker, error) {
    panic("implement me")
}

func (s *etcdv3Impl) List(dir string) ([]*storage.KVPair, error) {
    ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
    defer cancel()
    resp, err := s.client.Get(ctx, dir, v3.WithPrefix())
    if err != nil {
        return nil, err
    }

    kvs := make([]*storage.KVPair, 0, len(resp.Kvs))
    for _, kv := range resp.Kvs {
        kvs = append(kvs, &storage.KVPair{
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

func (s *etcdv3Impl) AtomicPut(key string, value []byte, previous *storage.KVPair, options *storage.WriteOptions) (bool, *storage.KVPair, error) {
    panic("implement me")
}

func (s *etcdv3Impl) AtomicDelete(key string, previous *storage.KVPair) (bool, error) {
    panic("implement me")
}

func (s *etcdv3Impl) Close() {
    s.client.Close()
    close(s.done)
}
