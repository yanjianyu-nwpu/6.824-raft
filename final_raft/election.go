package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	//if atomic.LoadInt32(&rf.Term) == args.Term && atomic.LoadInt32(&rf.Role) == Leader {
	//	reply.Term = atomic.LoadInt32(&rf.Term)
	//	reply.IsVote = false
	//	reply.CommittedIndex = atomic.LoadInt32(&rf.CommittedIndex)
	//	return
	//}
	if atomic.LoadInt32(&rf.Term) <= args.Term &&
		atomic.LoadInt32(&rf.CommittedIndex) <= args.CommittedIndex &&
		args.LastAppliedIndex >= atomic.LoadInt32(&rf.LastAppliedIndex) {

		//fmt.Println("rf.LastAppend", rf.LastAppliedIndex, "fr.wef", rf.CommittedIndex)
		reply.IsVote = true

		if atomic.LoadInt32(&rf.Role) != Follower {
			atomic.StoreInt32(&rf.Role, Follower)
			atomic.StoreInt32(&rf.LastAppliedIndex, atomic.LoadInt32(&rf.CommittedIndex))
			atomic.StoreInt32(&rf.LeaderCommittedIndex, atomic.LoadInt32(&rf.CommittedIndex))
			rf.persist()
		}
		reply.CommittedIndex = atomic.LoadInt32(&rf.CommittedIndex)
		reply.Term = atomic.LoadInt32(&rf.Term)
	} else {
		reply.IsVote = false
		reply.Term = atomic.LoadInt32(&rf.Term)
		reply.CommittedIndex = atomic.LoadInt32(&rf.CommittedIndex)
	}
	rf.mu.Unlock()
}
func (rf *Raft) doRequestVote(name int, args RequestVoteArgs) {
	var reply RequestVoteReply

	reply.IsVote = false

	ok := rf.peers[name].Call("Raft.RequestVote", args, &reply)

	if ok && reply.IsVote {
		//看收到投票机票加一

		//fmt.Println("I Recive a Vote")
		atomic.AddInt32(&rf.VoteNum, 1)

	}
	if ok {
		//更新NextAppliedIndex
		atomic.StoreInt32(&rf.NextAppliedIndex[name], reply.CommittedIndex+1)
	}
	if ok && reply.IsVote == false && reply.Term >= atomic.LoadInt32(&rf.Term) {
		atomic.StoreInt32(&rf.Role, Follower)
		return
	}
	//如果过半，且第一次过半
	if atomic.LoadInt32(&rf.VoteNum) >= int32(rf.PeersNum/2+1) &&
		atomic.LoadInt32(&rf.Role) == Candidate {
		//上锁
		rf.mu.Lock()

		//再校验一次，因为上一次校验没上锁
		if atomic.LoadInt32(&rf.Role) == Candidate {

			//fmt.Println("I become Leader ", rf.me)
			atomic.StoreInt32(&rf.Role, Leader)
			atomic.AddInt32(&rf.Term, 1)

			//atomic.StoreInt32(&rf.LeaderCommittedIndex, atomic.LoadInt32(&rf.CommittedIndex))
			//atomic.StoreInt32(&rf.LeaderCommittedInde)
			//atomic.StoreInt32(&rf.LastAppliedIndex, atomic.LoadInt32(&rf.CommittedIndex))

			rf.persist()
			//通知心跳循环可以开始了
			//	fmt.Println("I become Leader", rf.me)
			if len(rf.IsLeader) == 0 {
				rf.IsLeader <- 1
			}
			//立马传一个心跳包给大伙，宣告称为leader
			if len(rf.HeartBeatCh) < 1 {
				rf.HeartBeatCh <- 1
			}
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) Election() {
	//等所有可以提交的log提交
	for atomic.LoadInt32(&rf.LeaderCommittedIndex) > atomic.LoadInt32(&rf.CommittedIndex) &&
		atomic.LoadInt32(&rf.LastAppliedIndex)-atomic.LoadInt32(&rf.CommittedIndex) > 0 {
		if len(rf.LogsCommitCh) == 0 {
			rf.LogsCommitCh <- 1
		}
		time.Sleep(10)
	}
	rf.mu.Lock()
	atomic.StoreInt32(&rf.Role, Candidate)
	args := RequestVoteArgs{}

	args.Term = atomic.LoadInt32(&rf.Term)

	args.Candidate = int32(rf.me)

	args.CommittedIndex = atomic.LoadInt32(&rf.CommittedIndex)

	args.LastAppliedIndex = atomic.LoadInt32(&rf.LastAppliedIndex)
	atomic.StoreInt32(&rf.VoteNum, 1)

	rf.mu.Unlock()
	for i := 0; i < int(rf.PeersNum); i++ {
		if i == rf.me {
			continue
		}
		go rf.doRequestVote(i, args)
	}
}

//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// Your code here (2A, 2B).
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		fmt.Println("Role", atomic.LoadInt32(&rf.Role), "rf.me", rf.me, "rf.Term",
			atomic.LoadInt32(&rf.Term), "rf.LastAPpleind", rf.LastAppliedIndex, "rf.Commad", rf.CommittedIndex)

		time.Sleep(time.Duration(int(atomic.LoadInt32(&rf.ElectionTime))) * time.Millisecond)
		if time.Now().UnixNano()-atomic.LoadInt64(&rf.LastMsgTime) > int64(atomic.LoadInt32(&rf.ElectionTime))*int64(time.Millisecond) {
			//fmt.Println("I am Ok", rf.me)
			//	fmt.Println("Electiong", rf.me)
			rf.Election()
		}
	}
}
