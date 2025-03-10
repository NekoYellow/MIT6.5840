package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term

	// others
	role       RoleType // state of the server
	receivedHB bool     // if this server received heartbeat from leader this term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Check if a leader election should be started.
		if rf.role != LEADER && !rf.receivedHB {
			rf.mu.Lock()
			rf.currentTerm++
			rf.role = CANDIDATE
			rf.mu.Unlock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			voteCount := atomic.Int32{}
			voteCount.Store(1)
			wg := sync.WaitGroup{}
			for i := range len(rf.peers) {
				if i == rf.me {
					continue
				}

				wg.Add(1)
				go func(peer int) {
					defer wg.Done()
					reply := RequestVoteReply{}
					if !rf.sendRequestVote(peer, &args, &reply) {
						// fmt.Println("Call RequestVote failed with args", args)
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted {
						voteCount.Add(1)
						if int(voteCount.Load()) > len(rf.peers)/2 {
							rf.role = LEADER
						}
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = FOLLOWER
					}
				}(i)
			}
			wg.Wait()

			rf.mu.Lock()
			if rf.role == LEADER {
				rf.mu.Unlock()
				args := AppendEntriesArgs{
					Term: rf.currentTerm,
				}
				for i := range len(rf.peers) {
					if i == rf.me {
						continue
					}
					go func(peer int) {
						reply := AppendEntriesReply{}
						if !rf.sendAppendEntries(peer, &args, &reply) {
							// fmt.Println("Call AppendEntries failed with args", args)
						}
					}(i)
				}
				go rf.leaderTicker()
			} else {
				rf.mu.Unlock()
			}
		}

		rf.mu.Lock()
		rf.receivedHB = false
		rf.mu.Unlock()

		// pause for a random amount of time between 500 and 1500
		// milliseconds.
		ms := 500 + (rand.Int63() % 1000)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
		}
		wg := sync.WaitGroup{}
		for i := range len(rf.peers) {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(peer int) {
				defer wg.Done()
				reply := AppendEntriesReply{}
				if !rf.sendAppendEntries(peer, &args, &reply) {
					// fmt.Println("Call AppendEntries failed with args", args)
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
				}
			}(i)
		}
		wg.Wait()

		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		// the tester limits tens of heartbeats per second
		time.Sleep(time.Duration(101) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = FOLLOWER
	rf.receivedHB = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
