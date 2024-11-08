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
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
	// "fmt"
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
	timeout   int
	nextIndex []int
	matchIndex []int
	votedFor  int
}

type LogEntry struct {
	Term 	  int
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.term
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[len(rf.log) - 1].Term
	votedFor := rf.votedFor
	rf.mu.Unlock()
	if args.Term < reply.Term || (args.Term == reply.Term && votedFor >= 0 && votedFor != args.CandidateId) {
		reply.VoteGranted = false
	} else if args.Term == reply.Term {
		if votedFor < 0 || votedFor == args.CandidateId {
			rf.timer = time.Now()
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > reply.Term {
		if args.LastLogIndex >= lastIndex && args.LastLogTerm >= lastTerm {
			rf.timer = time.Now()
			rf.mu.Lock()
			rf.term = args.Term
			rf.state = 0
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			reply.VoteGranted = true
		} else {
			rf.mu.Lock()
			rf.term = args.Term
			rf.state = 0
			rf.votedFor = -1
			rf.mu.Unlock()
			reply.VoteGranted = false
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
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
		}
		rf.mu.Unlock()
		return 0
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
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
	timeout := rand.Intn(500) + 1000
	rf.mu.Lock()
	timediff := time.Since(rf.timer).Milliseconds()
	rf.mu.Unlock()
	for int(timediff) < timeout {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
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
	timeout := rand.Intn(500) + 1000
	electionTimer := time.Now()
	go rf.runElection(win)
	select {
	case <- win:		
		rf.mu.Lock()
		for server, _ := range rf.peers {
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = 0
		}
		rf.state = 2
		rf.mu.Unlock()
		rf.heartbeats()
	case <- time.After(time.Duration(timeout) * time.Millisecond):
		rf.mu.Lock()
		if rf.timer.After(electionTimer) {
			rf.state = 0
		}
		// if int(time.Since(rf.timer).Milliseconds()) < timeout {
		// 	rf.state = 0
		// }
		rf.mu.Unlock()
	}
}

func (rf *Raft) runElection(win chan bool) {
	rf.mu.Lock()
	rf.term += 1
	rf.votedFor = rf.me
	args := RequestVoteArgs{rf.term, rf.me, len(rf.log)-1, rf.log[len(rf.log)-1].Term}
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
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {
			rf.sendAppendEntries(i, []LogEntry{})
		}(index)
	}
	time.Sleep(120 * time.Millisecond)
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
	rf.log = []LogEntry{LogEntry{0}}
	rf.term = 0
	rf.state = 0
	rf.timer = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.term
	if rf.term > args.Term {
		reply.Success =	false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = 0
	}
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success =	false
		rf.mu.Unlock()
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
		if args.LeaderCommit < len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, entries []LogEntry) bool {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	args := AppendEntriesArgs{rf.term, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, entries, rf.commitIndex}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	// ok := false
	// for !ok {
	// 	ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return false
	}
	for !reply.Success {
		if reply.Term > args.Term {
			rf.mu.Lock()
			rf.state = 0
			rf.term = reply.Term
			rf.mu.Unlock()
			return false
		}
		prevLogIndex -= 1
		rf.mu.Lock()
		entries = append([]LogEntry{rf.log[prevLogIndex + 1]}, entries...)
		args := AppendEntriesArgs{rf.term, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, entries, rf.commitIndex}
		rf.mu.Unlock()
		reply = AppendEntriesReply{}
		rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	}
	rf.mu.Lock()
	rf.nextIndex[server] = prevLogIndex + len(entries) + 1
	rf.matchIndex[server] = prevLogIndex + len(entries)
	rf.mu.Unlock()
	return true
}