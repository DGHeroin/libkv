package libkv

type KVPair struct {
	Key       string
	Value     []byte
	LastIndex uint64
}
