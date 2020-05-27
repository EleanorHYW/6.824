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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Index 	int
	Command interface{}
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const (
	heartBeatTimeout = 150 * time.Millisecond
	electionTimeoutBase = 300 * time.Millisecond
)

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

	role 			Role
	voteCount		int

	// persistent
	currentTerm		int
	votedFor		int		// -1 for null
	log      		[]LogEntry

	// volatile on all servers
	commitIndex     int
	lastApplied		int

	// volatile on leaders
	nextIndex       []int
	matchIndex      []int

	// Timers
	chanGrantVote	chan bool
	chanWinElect	chan bool
	chanHeartbeat	chan bool

	// applyChan
	applyChan		chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	rf.mu.Unlock()
	return term, isLeader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

func (rf *Raft) getLastLogTermIndex() (int, int) {
	l := len(rf.log) - 1
	return rf.log[l].Term, rf.log[l].Index
}

func (rf *Raft) isUptoDate(candidateTerm int, candidateIndex int) bool {
	// 5.4.2
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	term, index := rf.getLastLogTermIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d, term %d receiving RequestVote: from %d, term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	// always update reply.term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("RequestVote: case 1\n")
		return
	} else if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		DPrintf("RequestVote: case 2\n")
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	DPrintf("current vote for %d\n", rf.votedFor)
	DPrintf("%v", rf.isUptoDate(args.LastLogTerm, args.LastLogIndex))
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUptoDate(args.LastLogTerm, args.LastLogIndex) {
		DPrintf("%d vote for %d\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if return
	if ok {
		if rf.role != Candidate || rf.currentTerm != args.Term {
			return ok
		}
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = -1
			return ok
		}

		// If votes received from majority of servers: become leader
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				DPrintf("%d win election\n", rf.me)
				rf.role = Leader
				rf.chanWinElect <- true
			}
		}
	}


	return ok
}


func (rf *Raft) sendRequestVoteToPeers() {
	rf.mu.Lock()
	DPrintf("%d sendRequestVoteToPeers\n", rf.me)
	lastLogIndex, lastLogTerm := rf.getLastLogTermIndex()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	// send requestVote to all peers
	for i := range rf.peers {
		// if have granted leader, return
		if rf.role != Candidate {
			break
		}
		if i != rf.me {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
	NextTryIndex 	int
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft)hasEntryConflict(serverLog []LogEntry, leaderLog []LogEntry) bool {
	l := min(len(leaderLog), len(serverLog))
	for i := 0; i < l; i++ {
		if serverLog[i].Term != leaderLog[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("server: %d, role: %d, lastApplied %d, commitIndex %d, logs: %v\n", rf.me, rf.role, rf.lastApplied, rf.commitIndex, rf.log)

	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// always update reply.term
	reply.Term = rf.currentTerm
	reply.Success = false

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	// reply.Success = true
	rf.chanHeartbeat <- true

	_, lastLogIndex := rf.getLastLogTermIndex()

	// completely out of date
	if args.PrevLogIndex > lastLogIndex {
		reply.NextTryIndex = lastLogIndex + 1
		return
	}

	// exist log index, term mismatch
	// # 5.3
	// For example, when rejecting an AppendEntries request, the follower
	// can include the term of the conflicting entry and the first
	// index it stores for that term. With this information, the
	// leader can decrement nextIndex to bypass all of the conflicting entries in that term;
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		//fmt.Println(args.PrevLogIndex)
		pos := args.PrevLogIndex - 1
		for pos >= 0 {
			if rf.log[pos].Term != rf.log[args.PrevLogIndex].Term {
				break
			}
			pos--
		}
		reply.NextTryIndex = pos + 1
		////fmt.Printf("%d reply %d", rf.me, args.LeaderId)
		//fmt.Println(reply.NextTryIndex)
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it

	localCommitedlog := rf.log[:args.PrevLogIndex + 1]
	restLog := rf.log[args.PrevLogIndex + 1:]

	// simply update append any new entries not already in the log
	if rf.hasEntryConflict(restLog, args.Entries) || len(args.Entries) > len(restLog) {
		rf.log = append(localCommitedlog, args.Entries...)
	}

	// do noting otherwise
	reply.Success = true
	reply.NextTryIndex = rf.log[len(rf.log) - 1].Index

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
		//fmt.Printf("leader %d, me %d, args.LeaderCommit: %d, rf.commitIndex: %d\n", args.LeaderId, rf.me, args.LeaderCommit, rf.commitIndex)
		go rf.commitLog()
	}
	// commit
	// go rf.commitLog()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = -1
			return ok
		}

		// If successful: update nextIndex and matchIndex for
		// follower
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else {
			// If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry
			rf.nextIndex[server] = reply.NextTryIndex
		}

		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		_, N := rf.getLastLogTermIndex()
		flag := false
		for N > rf.commitIndex && rf.log[N].Term == rf.currentTerm && !flag {
			cnt := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					cnt++
				}
				if cnt > len(rf.peers) / 2 {
					rf.commitIndex = N
					flag = true
					go rf.commitLog()
					break
				}
			}
			N--
		}

	}

	return ok
}

func (rf *Raft) sendAppendEntriesToPeers() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// send requestVote to all peers
	for i := range rf.peers {
		// if have granted leader, return
		if rf.role != Leader {
			break
		}
		if i != rf.me {
			DPrintf("%d sendAppendEntriesToPeers %d\n", rf.me, i)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i] - 1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			_, lastLogIndex := rf.getLastLogTermIndex()
			if rf.nextIndex[i] <= lastLogIndex {
				args.Entries = rf.log[args.PrevLogIndex + 1:]
			}
			//fmt.Printf("leader %d send %d log %v\n", rf.me, i, args.Entries)
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == Leader

	// if this
	// server isn't the leader, returns false
	if isLeader {
		// the first return value is the index that the command will appear at
		// if it's ever committed. the second return value is the current
		// term. the third return value is true if this server believes it is
		// the leader.
		_, lastLogIndex := rf.getLastLogTermIndex()
		index = lastLogIndex + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
	}

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

	rf.role = Follower
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term:0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.chanWinElect = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.role {
			case Follower:
				select {
				case <-rf.chanGrantVote:
				case <-rf.chanHeartbeat:
				// If election timeout elapses without receiving AppendEntries
				// RPC from current leader or granting vote to candidate:
				// convert to candidate
				case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)) + electionTimeoutBase):
					rf.role = Candidate
					DPrintf("%d turn to candidate\n", rf.me)
				}
			case Leader:
				// send initial empty AppendEntries RPCs
				// (heartbeat) to each server; repeat
				rf.sendAppendEntriesToPeers()
				time.Sleep(heartBeatTimeout)
			case Candidate:
				// On conversion to candidate, start election
				// Increment currentTerm
				// Vote for self
				// Reset election timer
				// Send RequestVote RPCs to all other servers
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.mu.Unlock()
				go rf.sendRequestVoteToPeers()

				select {
				// If AppendEntries RPC received from new leader: convert to
				// follower
				case <-rf.chanHeartbeat:
					rf.role = Follower
				case <-rf.chanWinElect:
					// for each server, index of the next log entry
					// to send to that server (initialized to leader
					// last log index + 1)
					// Reinitialized after election
					rf.mu.Lock()
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					_, lastLogIndex := rf.getLastLogTermIndex()
					nextIndex := lastLogIndex + 1
					for i := range rf.nextIndex {
						rf.nextIndex[i] = nextIndex
					}
					rf.mu.Unlock()
					//fmt.Printf("%d win elect\n", rf.me)
				// If election timeout elapses: start new election
				case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)) + electionTimeoutBase):
				}
			}
		}
	}()

	return rf
}
