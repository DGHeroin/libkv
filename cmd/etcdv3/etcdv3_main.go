package main

import (
    "github.com/DGHeroin/libkv/etcdv3"
    "log"
)

func main()  {
    log.Println()
    kv, err := etcdv3.New([]string{"127.0.0.1:2379"}, nil)
    if err != nil {
        log.Fatal(err)
    }

    kv.Put("hello", []byte("world"), nil)
    resp, _ := kv.Get("hello")
    log.Printf("==>%s, %v", resp.Value, resp.LastIndex)
}
