package main

import (
	_ "github.com/DGHeroin/libkv/etcdv3"
	"github.com/DGHeroin/libkv/kv"
	_ "github.com/DGHeroin/libkv/leveldb"
	"log"
	"time"
)

func main()  {
	db, err := kv.New("etcdv3", []string{"localhost:2379"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	key := "hello"
	err = db.Put(key, []byte("sss"), nil)
	if err != nil {
		log.Fatal(err)
	}

	r, err := db.Get(key)
	log.Println("result:", r.Value)

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(time.Millisecond)
			db.Put(key, []byte(time.Now().String()), nil)
		}
	}()


	ch, err := db.Watch(key, nil)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case d := <-ch:
			log.Printf("====>%s", d.Value)
		}
	}
}
