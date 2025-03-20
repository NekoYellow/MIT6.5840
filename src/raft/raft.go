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
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logs        []LogEntry // log entries

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// others
	role           RoleType      // state of the server
	electionTimer  *time.Timer   // timer for election timeout
	heartbeatTimer *time.Timer   // timer for heartbeat
	applyCh        chan ApplyMsg // channel to send apply message to service
	applyCond      *sync.Cond    // condition variable for apply goroutine
	replicatorCond []*sync.Cond  // condition variable for replicator goroutine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.role == LEADER
}

func (rf *Raft) OnChange(newRole RoleType) {
	if rf.role == newRole {
		return
	}
	rf.role = newRole
	switch newRole {
	case LEADER:
		rf.electionTimer.Stop()                           // stop election
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout()) // start heartbeat
		rf.BroadcastHeartbeat(false)                      // -- immediately
	case FOLLOWER:
		rf.electionTimer.Reset(RandomElectionTimeout()) // stay good
		rf.heartbeatTimer.Stop()                        // stop heartbeat
	case CANDIDATE:
	}
}

func (rf *Raft) firstLog() LogEntry {
	return rf.logs[0]
}

func (rf *Raft) lastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) logWithIndex(i int) LogEntry {
	return rf.logs[i-rf.firstLog().Index]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(logs) != nil {
		DPrintf("{Node %v} fails to decode persisted state", rf.me)
	} else {
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
		rf.lastApplied, rf.commitIndex = rf.firstLog().Index, rf.firstLog().Index
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return -1, -1, false // index, term, isLeader
	}
	newLogIndex := rf.lastLog().Index + 1
	rf.logs = append(rf.logs, LogEntry{ // append entry locally
		Term:    rf.currentTerm,
		Command: command,
		Index:   newLogIndex,
	})
	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLogIndex, newLogIndex+1
	rf.BroadcastHeartbeat(true) // broadcast entry to all peers
	return newLogIndex, rf.currentTerm, true

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

func (rf *Raft) StartElection() {
	rf.votedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLog().Index,
		LastLogTerm:  rf.lastLog().Term,
	}
	votesCount := atomic.Int32{}
	votesCount.Store(1)
	// wg := sync.WaitGroup{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// wg.Add(1)
		go func(peer int) {
			// defer wg.Done()
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != args.Term || rf.role != CANDIDATE {
				return
			}
			if reply.VoteGranted {
				votesCount.Add(1)
				if int(votesCount.Load()) > len(rf.peers)/2 {
					rf.OnChange(LEADER)
				}
			} else if reply.Term > rf.currentTerm {
				rf.OnChange(FOLLOWER)
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
			}
		}(i)
	}
	// wg.Wait()
}

func (rf *Raft) BroadcastHeartbeat(containsEntries bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if containsEntries {
			rf.replicatorCond[i].Signal()
		} else {
			go rf.replicateOnce(i)
		}
	}
}

func (rf *Raft) replicateOnce(peer int) {
	rf.mu.RLock()
	if rf.role != LEADER {
		rf.mu.RUnlock()
		return
	}
	prevIndex := rf.nextIndex[peer] - 1
	firstIndex := rf.firstLog().Index
	entries := make([]LogEntry, len(rf.logs[prevIndex+1-firstIndex:]))
	copy(entries, rf.logs[prevIndex+1-firstIndex:])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.logWithIndex(prevIndex).Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.RUnlock()
	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, &args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term || rf.role != LEADER {
		return
	}
	if !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.OnChange(FOLLOWER)
			rf.currentTerm, rf.votedFor = reply.Term, -1
			rf.persist()
		} else if reply.Term == rf.currentTerm {
			rf.nextIndex[peer] = reply.ConflictIndex
			if reply.ConflictTerm == -1 {
				return
			}
			// find largest i s.t. rf.logWithIndex(i).Term == reply.ConflictTerm
			L, R := firstIndex, prevIndex
			for L < R {
				M := (L + R + 1) / 2
				if rf.logWithIndex(M).Term <= reply.ConflictTerm {
					L = M
				} else {
					R = M - 1
				}
			}
			if rf.logWithIndex(L).Term == reply.ConflictTerm {
				rf.nextIndex[peer] = L
			}
		}
	} else {
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// advance commitIndex if possible
		n := len(rf.matchIndex)
		sortMatchIndex := make([]int, n)
		copy(sortMatchIndex, rf.matchIndex)
		sort.Ints(sortMatchIndex)
		// get the index of the log entry with the highest index that is known to be replicated on a majority of servers
		newCommitIndex := sortMatchIndex[n-(n/2+1)]
		if newCommitIndex > rf.commitIndex {
			// if log is matched
			if newCommitIndex <= rf.lastLog().Index && rf.logWithIndex(newCommitIndex).Term == rf.currentTerm {
				rf.commitIndex = newCommitIndex
				rf.applyCond.Signal()
			}
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// check the logs of peer is behind the leader
	return rf.role == LEADER && rf.matchIndex[peer] < rf.lastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// send log entries to peer
		rf.replicateOnce(peer)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C: // election
			rf.mu.Lock()
			rf.OnChange(CANDIDATE)
			rf.currentTerm++
			rf.persist()
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout()) // in case of split vote
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C: // heartbeat
			rf.mu.Lock()
			if rf.role == LEADER {
				rf.BroadcastHeartbeat(false)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// check the commitIndex is advanced
		for rf.commitIndex <= rf.lastApplied {
			// need to wait for the commitIndex to be advanced
			rf.applyCond.Wait()
		}

		// apply log entries to state machine
		firstLogIndex, commitIndex, lastApplied := rf.firstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		// send the apply message to applyCh for service/State Machine Replica
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		// rf.commitIndex may change during the Unlock() and Lock()
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
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
	rf := &Raft{
		mu:        sync.RWMutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]LogEntry, 1),

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		role:           FOLLOWER,
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// use mu to protect applyCond
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize nextIndex and matchIndex, and start replicator goroutine
	for i := range peers {
		rf.matchIndex[i], rf.nextIndex[i] = 0, rf.lastLog().Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to send log entries to peer
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to catch timer timeouts
	go rf.ticker()
	// start apply goroutine to apply log entries to state machine
	go rf.applier()

	return rf
}
