package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

const (
	Leader        int32 = 1
	Follower      int32 = 0
	Candidate     int32 = 2
	HeartBeatTime int32 = 120

	InitLogsLength int32 = 20000
	AddLogsLength  int32 = 1000
)

type Entery struct {
	Command interface{}
	Index   int32
	Term    int32
}

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
type AppendEnteryArgs struct {
	LeaderCommittedIndex int32
	PreLogIndex          int32
	PreLogTerm           int32

	Term     int32
	Enteries []Entery
}
type AppendEnteryReply struct {
	LastAppliedIndex int32
	Term             int32
	Success          bool
}
type RequestVoteReply struct {
	// Your data here (2A).
	IsVote bool
	Term   int32
	//Candidate      int
	CommittedIndex int32
}
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term           int32
	Candidate      int32
	CommittedIndex int32
	IsLeader       bool

	LastAppliedIndex int32
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	Role int32

	LastMsgTime int64

	PeersNum int32

	ElectionTime int32

	Term int32

	VoteFor int32

	VoteNum int32
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	LogsCommitCh         chan int32
	HeartBeatCh          chan int32
	MyNextAppliedIndexCh chan int32

	IsLeader chan int

	LogsMu sync.Mutex
	Logs   []Entery

	NextAppliedIndex     []int32
	LastAppliedIndex     int32
	CommittedIndex       int32
	LeaderCommittedIndex int32

	HeartBeatTimer *time.Ticker
}
