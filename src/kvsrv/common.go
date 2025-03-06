package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	IsAction bool
	OpID     int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
