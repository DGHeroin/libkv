package kv

import (
	"errors"
	"github.com/DGHeroin/libkv"
)

var (
	NewMap = map[string] func(endpoints[]string, opt *libkv.Options) (libkv.Store, error) {}
)

func New(name string, endpoints[]string, opt *libkv.Options) (libkv.Store, error) {
	cb, ok := NewMap[name]
	if ok {
		return cb(endpoints, opt)
	}
	return nil, errors.New("not support")
}
