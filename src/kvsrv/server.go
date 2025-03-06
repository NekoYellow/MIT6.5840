package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu  sync.Mutex
	mp  map[string]string
	buf sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.mp[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !args.IsAction {
		kv.buf.Delete(args.OpID)
		return
	}
	_, vis := kv.buf.Load(args.OpID)
	if vis {
		return
	}
	kv.mp[args.Key] = args.Value
	kv.buf.Store(args.OpID, args.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !args.IsAction {
		kv.buf.Delete(args.OpID)
		return
	}
	value, vis := kv.buf.Load(args.OpID)
	if vis {
		str, ok := value.(string) // type assertion
		if !ok {
			panic("value is not a string")
		}
		reply.Value = str
		return
	}
	oldValue := kv.mp[args.Key]
	kv.mp[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	kv.buf.Store(args.OpID, oldValue)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.mu = sync.Mutex{}
	kv.mp = map[string]string{}
	kv.buf = sync.Map{}

	return kv
}
