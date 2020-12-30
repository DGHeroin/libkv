package libkv

import (
    "fmt"
    "sort"
    "strings"
)

type Initialize func(endpoints []string, opt*Config) (Storage, error)

var (
    initializers     = make(map[string]Initialize)
    supportedStorage = func() string {
        keys := make([]string,0)
        for k := range initializers {
            keys = append(keys, string(k))
        }
        sort.Strings(keys)
        return strings.Join(keys, ", ")
    }
)

func NewStorage(name string, endpoints []string, opt*Config) (Storage, error)  {
    if cb , ok := initializers[name]; ok {
        return cb(endpoints, opt);
    }
    return nil, fmt.Errorf("%s %s", ErrStorageNotSupport, name)
}

func AddStorage(name string, fn Initialize)  {
    initializers[name] = fn
}