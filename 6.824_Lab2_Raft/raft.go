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
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
	"fmt"
)

const (
	APPLY_INTERVAL = 20
	COMMIT_INTERVAL = 20
	HEARTBEAT_INTERVAL = 120
	LISTEN_INTERVAL = 500
	LOWEST_TIMEOUT = 500
	TIMEOUT_RANGE = 1000
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term 	  int
	state     int8
	log 	  []LogEntry
	timer     time.Time
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	votedFor  int
	leaderCh  chan bool
}

type LogEntry struct {
	Term 	  int
	Command   interface{}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.log = []LogEntry{LogEntry{0, nil}}
	rf.term = 0
	rf.state = 0
	rf.timer = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.leaderCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply(applyCh)
	go rf.leaderCommit()

	return rf
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	} else {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{rf.term, command})
		rf.persist()
		index := len(rf.log) - 1
		rf.mu.Unlock()
		go rf.replicateLog(false)
		return index, term, isLeader
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.term
	state := rf.state
	rf.mu.Unlock()
	return term, state == 2
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	return state == 2
}

func (rf *Raft) takeOffice() {
	rf.mu.Lock()
	for server, _ := range rf.peers {
		rf.nextIndex[server] = len(rf.log)
		rf.matchIndex[server] = 0
	}
	rf.state = 2
	rf.mu.Unlock()
	rf.heartbeats()
	rf.leaderCh <- true
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
	  fmt.Println("decoding error!")
	} else {
	  rf.term = term
	  rf.votedFor = votedFor
	  rf.log = log
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) replicateLog(isHeartbeat bool) {
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			isLeader := rf.state == 2
			disagree := len(rf.log) > rf.nextIndex[i]
			term := rf.term
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			entries := rf.log[rf.nextIndex[i]:]
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			if isLeader && (disagree || isHeartbeat) {
				rf.sendAppendEntries(i, term, prevLogIndex, prevLogTerm, entries, commitIndex)
			}
		}(index)
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		entries := []LogEntry{}
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		if commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1: commitIndex+1]
		}
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.lastApplied += 1
			rf.applyEntry(rf.lastApplied, entry, applyCh)
		}
		time.Sleep(time.Duration(APPLY_INTERVAL) * time.Millisecond)
	}
}

func (rf *Raft) applyEntry(i int, entry LogEntry, applyCh chan ApplyMsg) {
	var msg ApplyMsg
	msg.CommandValid = true
	msg.Command = entry.Command
	msg.CommandIndex = i
	applyCh <- msg
}

func (rf *Raft) leaderCommit() {
	for rf.killed() == false {
		isLeader := <- rf.leaderCh
		if !isLeader {
			continue
		}
		for rf.isLeader() {
			rf.commit()
			time.Sleep(time.Duration(COMMIT_INTERVAL) * time.Millisecond)
		}
	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	newCommitIndex := rf.commitIndex
	for newCommitIndex < len(rf.log) - 1 {
		newCommitTerm := rf.log[newCommitIndex + 1].Term
		if newCommitTerm < rf.term {
			newCommitIndex += 1
			continue
		}
		count := 1
		for i, index := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if index > newCommitIndex {
				count += 1
			}
		}
		if 2 * count <= len(rf.peers) {
			break
		}
		newCommitIndex += 1
	}
	if rf.log[newCommitIndex].Term == rf.term && newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
	}
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case 0:
			rf.listens()
		case 1:
			rf.election()
		case 2:
			rf.heartbeats()
		}
	}
}

func (rf *Raft) listens() {
	timeout := rand.Intn(TIMEOUT_RANGE) + LOWEST_TIMEOUT
	rf.mu.Lock()
	timediff := time.Since(rf.timer).Milliseconds()
	rf.mu.Unlock()
	for int(timediff) < timeout {
		time.Sleep(time.Duration(LISTEN_INTERVAL) * time.Millisecond)
		rf.mu.Lock()
		timediff = time.Since(rf.timer).Milliseconds()
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.state = 1
	rf.mu.Unlock()
}

func (rf *Raft) election() {
	win := make(chan bool)
	timeout := rand.Intn(TIMEOUT_RANGE) + LOWEST_TIMEOUT
	electionTimer := time.Now()
	go rf.runElection(win)
	select {
	case <- win:		
		rf.takeOffice()
	case <- time.After(time.Duration(timeout) * time.Millisecond):
		rf.mu.Lock()
		if rf.timer.After(electionTimer) {
			rf.state = 0
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) runElection(win chan bool) {
	rf.mu.Lock()
	rf.term += 1
	rf.votedFor = rf.me
	args := RequestVoteArgs{rf.term, rf.me, len(rf.log)-1, rf.log[len(rf.log)-1].Term}
	rf.persist()
	rf.mu.Unlock()
	votes := 1
	var voteMu sync.Mutex
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {
			vote := rf.sendRequestVote(i, &args)
			voteMu.Lock()
			votes += vote
			if 2 * votes > len(rf.peers) {
				win <- true
			}
			voteMu.Unlock()
		}(index)
	}
}

func (rf *Raft) heartbeats() {
	rf.replicateLog(true)
	time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
}

type AppendEntriesArgs struct {
	Term int	
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	SuggestIndex int
	SuggestTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term {
		reply.Term = rf.term
		reply.Success =	false
		return
	}
	rf.state = 0
	if args.PrevLogIndex >= len(rf.log) {
		reply.SuggestIndex = len(rf.log) - 1
		reply.SuggestTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term < args.PrevLogTerm {
		reply.SuggestIndex = args.PrevLogIndex - 1
		reply.SuggestTerm = rf.log[args.PrevLogIndex - 1].Term
		return
	} else if rf.log[args.PrevLogIndex].Term > args.PrevLogTerm {
		SuggestIndex := args.PrevLogIndex
		for rf.log[SuggestIndex].Term > args.PrevLogTerm {
			SuggestIndex -= 1
		}
		reply.SuggestIndex = SuggestIndex
		reply.SuggestTerm = rf.log[SuggestIndex].Term
		return
	}
	rf.timer = time.Now()
	for i, entry := range args.Entries {
		if args.PrevLogIndex + 1 + i < len(rf.log) {
			if rf.log[args.PrevLogIndex + 1 + i].Term != entry.Term {
				rf.log = rf.log[:args.PrevLogIndex + 1 + i]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < args.PrevLogIndex + len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
	}
	rf.term = args.Term
	rf.persist()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []LogEntry, commitIndex int) bool {
	args := AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, entries, commitIndex}
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		rf.nextIndex[server] = prevLogIndex + len(entries) + 1
		rf.matchIndex[server] = prevLogIndex + len(entries)
		return true
	} else {
		if reply.Term > rf.term {
			rf.state = 0
			rf.term = reply.Term
			rf.persist()
			return false
		}
		if rf.term != args.Term {
			return false
		}
		newPrevIndex := reply.SuggestIndex
		if rf.log[newPrevIndex].Term < reply.SuggestTerm {
			newPrevIndex -= 1
		} else if reply.SuggestTerm >= 0 && rf.log[newPrevIndex].Term > reply.SuggestTerm {
			for rf.log[newPrevIndex].Term > reply.SuggestTerm {
				newPrevIndex -= 1
			}
		}
		rf.nextIndex[server] = newPrevIndex + 1
		return false
	}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[len(rf.log) - 1].Term
	if args.Term < rf.term {
		rf.persist()
		reply.Term = rf.term
		reply.VoteGranted = false
	} else if args.Term == rf.term {
		if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
		} else if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
			rf.votedFor = -1
			rf.persist()
			reply.VoteGranted = false
		} else {
			rf.timer = time.Now()
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		}
	} else if args.Term > rf.term {
		if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
			rf.term = args.Term
			rf.state = 0
			rf.votedFor = -1
			rf.persist()
			reply.VoteGranted = false
		} else {
			rf.timer = time.Now()
			rf.term = args.Term
			rf.state = 0
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) int {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.VoteGranted {
		return 1
	} else {
		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.state = 0
			rf.term = reply.Term
			rf.persist()
		}
		rf.mu.Unlock()
		return 0
	}
}