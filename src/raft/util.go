package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command any // command for state machine
	Term    int // term when entry was received by leader
	Index   int // index of the entry in log
}

type RoleType int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func RandomElectionTimeout() time.Duration {
	return time.Duration(1000+rand.Int63()%1000) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(125) * time.Millisecond
}
