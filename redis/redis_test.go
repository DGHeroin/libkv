package redis

import (
    "github.com/stretchr/testify/assert"
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
            t.Logf("================>%s", val.Value)
        }
    }

}
