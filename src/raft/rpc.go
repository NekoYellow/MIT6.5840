package raft

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.OnChange(FOLLOWER)
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	if rf.lastLog().Term > args.LastLogTerm ||
		(rf.lastLog().Term == args.LastLogTerm && rf.lastLog().Index > args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// --------------------------------------------------------------------------------------------- //

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of candidate's last log entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // index of the first conflict log entry
	ConflictTerm  int  // term of the first conflict log entry
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	lastIndex := rf.lastLog().Index
	firstIndex := rf.firstLog().Index

	// peer is the leader
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.OnChange(FOLLOWER)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// log doesn't contain the matching entry
	if args.PrevLogIndex < firstIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// log has conflict
	if args.PrevLogIndex > lastIndex ||
		args.PrevLogTerm != rf.logWithIndex(args.PrevLogIndex).Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PrevLogIndex > lastIndex {
			reply.ConflictIndex = lastIndex + 1
			reply.ConflictTerm = -1
		} else {
			i := args.PrevLogIndex
			for i >= firstIndex && rf.logWithIndex(i).Term == args.PrevLogTerm { // curious here
				i--
			}
			reply.ConflictIndex = i + 1
			reply.ConflictTerm = args.PrevLogTerm
		}
		return
	}

	// append new entries
	for i, entry := range args.Entries {
		if entry.Index > lastIndex || rf.logWithIndex(entry.Index).Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstIndex], args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
		rf.applyCond.Signal()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

// send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
