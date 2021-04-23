package redis

import (
    "github.com/stretchr/testify/assert"
    "log"
    "testing"
    "time"
)

func TestNew(t *testing.T) {
    kv, err := New([]string{"127.0.0.1:6379"}, nil)
    defer kv.Close()
    assert.Nil(t, err)
    key := "Helloccc"
    ok, err := kv.Exists(key)
    t.Logf("exist[%v] %v", ok, err)
    kv.Put(key, []byte("world"), nil)
    
    ok, err = kv.Exists(key)
    t.Logf("exist[%v] %v", ok, err)
    
    kv.Delete(key)
}

func TestWatch(t *testing.T) {
    key := "/test_dir/node1"
    kv, err := New([]string{"127.0.0.1:6379"}, nil)
    defer kv.Close()
    assert.Nil(t, err)
    
    ch, err := kv.Watch(key, nil)
    if err != nil {
        t.Log(err)
    } else {
        go func() {
            time.Sleep(time.Second)
            err = kv.Put(key, []byte("value1"), nil)
            assert.Nil(t, err)
            
            time.Sleep(time.Second)
            err = kv.Put(key, []byte("value2"), nil)
            assert.Nil(t, err)
            
            time.Sleep(time.Second)
            err = kv.Put(key, []byte("value3"), nil)
            assert.Nil(t, err)
            //kv.Delete(key)
        }()
    }

    for {
        select {
        case <-time.After(time.Second * 5):
            return
        case val := <-ch:
            if val == nil {
                log.Println("quit")
                break
            }
            t.Logf("================>%s => %s", val.Key, val.Value)
        }
    }
}

func TestWatchTree(t *testing.T) {
    dir := "/test_dir/services/"
    kv, err := New([]string{"127.0.0.1:6379"}, nil)
    defer kv.Close()
    assert.Nil(t, err)

    ch, err := kv.WatchTree(dir, nil)
    if err != nil {
        t.Log(err)
    } else {
        go func() {
            time.Sleep(time.Second)
            err = kv.Put(dir + "1", []byte("value1"), nil)
            assert.Nil(t, err)

            time.Sleep(time.Second)
            err = kv.Put(dir + "2", []byte("value2"), nil)
            assert.Nil(t, err)

            time.Sleep(time.Second)
            err = kv.Put(dir + "3", []byte("value3"), nil)
            assert.Nil(t, err)
            //kv.Delete(key)
        }()
    }

    for {
        select {
        case <-time.After(time.Second * 5):
            return
        case kvs := <-ch:
            if kvs == nil {
                break
            }
            log.Println("长度", len(kvs))
            for _, val := range kvs {
                t.Logf("================>%s =>%s ", val.Key, val.Value)
            }

        }
    }
}
