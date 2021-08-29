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

	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = int(atomic.LoadInt32(&rf.Term))
	isleader = (atomic.LoadInt32(&rf.Role) == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(atomic.LoadInt32(&rf.CommittedIndex))
	e.Encode(atomic.LoadInt32(&rf.LeaderCommittedIndex))
	e.Encode(atomic.LoadInt32(&rf.LastAppliedIndex))
	e.Encode(atomic.LoadInt32(&rf.Term))
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.CommittedIndex)
	d.Decode(&rf.LeaderCommittedIndex)
	d.Decode(&rf.LastAppliedIndex)
	d.Decode(&rf.Term)
	d.Decode(&rf.Logs)

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
	return ok
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
	//fmt.Println("Command", command)
	if atomic.LoadInt32(&rf.Role) != Leader {
		return -1, -1, false
	}

	rf.mu.Lock()
	if atomic.LoadInt32(&rf.Role) != Leader ||
		atomic.LoadInt32(&rf.LastAppliedIndex)-atomic.LoadInt32(&rf.CommittedIndex) > 0 {
		rf.mu.Unlock()
		return -1, -1, false
	}
	index = int(atomic.AddInt32(&rf.LastAppliedIndex, 1))
	term = int(atomic.LoadInt32(&rf.Term))

	if len(rf.Logs) < index {
		e := make([]Entery, 10000)
		rf.Logs = append(rf.Logs, e...)
	}

	rf.Logs[index].Command = command
	rf.Logs[index].Term = int32(term)

	rf.persist()
	//fmt.Println("Start rf.me", rf.me, "Index ", index, "Term", term, "Command", command)
	//这里到时候再看看怎么用channel
	if len(rf.HeartBeatCh) < 1 {
		rf.HeartBeatCh <- 1
	}
	rf.mu.Unlock()

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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	atomic.StoreInt32(&rf.Role, Candidate)

	atomic.StoreInt32(&rf.Term, 0)

	atomic.StoreInt32(&rf.PeersNum, int32(len(peers)))

	rand.Seed(int64(me))
	atomic.StoreInt32(&rf.ElectionTime, int32(rand.Intn(50)+150))

	atomic.StoreInt32(&rf.VoteNum, 0)

	atomic.StoreInt32(&rf.VoteFor, -1)
	// initialize from state persisted before a crash

	atomic.StoreInt64(&rf.LastMsgTime, time.Now().UnixNano())
	rf.applyCh = applyCh

	rf.LogsCommitCh = make(chan int32, 1)
	rf.HeartBeatCh = make(chan int32, 1)
	rf.MyNextAppliedIndexCh = make(chan int32, 1)

	rf.IsLeader = make(chan int, 1)
	rf.MyNextAppliedIndexCh <- 1

	atomic.StoreInt32(&rf.CommittedIndex, 0)
	atomic.StoreInt32(&rf.LeaderCommittedIndex, 0)
	atomic.StoreInt32(&rf.LastAppliedIndex, 0)

	rf.LogsMu.Lock()
	rf.Logs = make([]Entery, InitLogsLength)
	rf.LogsMu.Unlock()
	// start ticker goroutine to start elections

	rf.NextAppliedIndex = make([]int32, rf.PeersNum)
	for i := 0; i < int(rf.PeersNum); i++ {
		rf.NextAppliedIndex[i] = 1
	}

	rf.HeartBeatTimer = nil

	rf.readPersist(persister.ReadRaftState())
	go rf.LeaderHeart()
	go rf.ticker()
	go rf.LogCommittedCircle()
	return rf
}
