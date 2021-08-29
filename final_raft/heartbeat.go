package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (rf *Raft) LeaderHeart() {
	//rf.HeartBeatTimer.reset(int(HeartBeatTime)*time.Millisecond)

	for rf.killed() == false {
		if atomic.LoadInt32(&rf.Role) == Leader {
			select {
			case <-rf.HeartBeatTimer.C:
				if atomic.LoadInt32(&rf.Role) == Leader {
					rf.BroadCast()
				}
			case <-rf.HeartBeatCh:
				if atomic.LoadInt32(&rf.Role) == Leader {
					rf.BroadCast()
				}
			}
			//time.Sleep(time.Duration(int(HeartBeatTime)) * time.Millisecond)
		} else {
			if rf.HeartBeatTimer != nil {
				rf.HeartBeatTimer.Stop()
				fmt.Println("A Leader is over")
			}
			//fmt.Println("Committed Circle is OK")
			<-rf.IsLeader
			//rf.HeartBeatCh <- 1
			rf.HeartBeatTimer = time.NewTicker(100 * time.Millisecond)
			//rf.HeartBeatTimer = time.NewTicker(20 * time.Millisecond)
		}
	}
}
func (rf *Raft) HandleHeartBeats(args AppendEnteryArgs, reply *AppendEnteryReply) {
	//defer rf.mu.Unlock()
	if atomic.LoadInt32(&rf.Term) > args.Term {
		reply.Success = false
		reply.LastAppliedIndex = atomic.LoadInt32(&rf.LastAppliedIndex)
		reply.Term = atomic.LoadInt32(&rf.Term)
		return
	}

	//fmt.Println("I get HeartBeat,  rf.me", rf.me)
	atomic.StoreInt64(&rf.LastMsgTime, time.Now().UnixNano())

	if atomic.LoadInt32(&rf.Role) != Follower {

		rf.mu.Lock()
		//fmt.Println("I become Followr rf,me", rf.me)
		//fmt.Println("I am Here C", rf.me)
		atomic.StoreInt32(&rf.Role, Follower)
		atomic.StoreInt32(&rf.Term, args.Term)
		atomic.StoreInt32(&rf.LastAppliedIndex, atomic.LoadInt32(&rf.CommittedIndex))
		atomic.StoreInt32(&rf.LeaderCommittedIndex, atomic.LoadInt32(&rf.CommittedIndex))

		reply.Success = false
		reply.LastAppliedIndex = atomic.LoadInt32(&rf.LastAppliedIndex)
		reply.Term = atomic.LoadInt32(&rf.Term)
		rf.mu.Unlock()
		return
	}
	//fmt.Println("I add Change Term rf.me", rf.me)
	atomic.StoreInt32(&rf.Term, args.Term)

	if args.PreLogIndex > atomic.LoadInt32(&rf.LastAppliedIndex) {

		//fmt.Println("I am Here B", rf.me)
		reply.LastAppliedIndex = atomic.LoadInt32(&rf.LastAppliedIndex)
		reply.Success = false
		reply.Term = atomic.LoadInt32(&rf.Term)
		return
	}

	rf.mu.Lock()
	if rf.Logs[args.PreLogIndex].Term != args.PreLogTerm {

		//fmt.Println("I am Here A", rf.me)
		reply.LastAppliedIndex = args.PreLogIndex - 1
		reply.Success = false
		reply.Term = atomic.LoadInt32(&rf.Term)
		rf.mu.Unlock()
		return
	}
	//fmt.Println("I Add Log rf.me", rf.me)
	argslen := len(args.Enteries)
	if len(rf.Logs)-int(args.PreLogIndex) < argslen+100 {
		e := make([]Entery, 1000)
		rf.Logs = append(rf.Logs, e...)
	}
	for i := 0; i < len(args.Enteries); i++ {
		rf.Logs[i+int(args.PreLogIndex)+1] = args.Enteries[i]
	}

	atomic.StoreInt32(&rf.LeaderCommittedIndex, args.LeaderCommittedIndex)
	atomic.StoreInt32(&rf.LastAppliedIndex, args.PreLogIndex+int32(len(args.Enteries)))

	reply.LastAppliedIndex = atomic.LoadInt32(&rf.LastAppliedIndex)
	reply.Success = true
	reply.Term = atomic.LoadInt32(&rf.Term)

	rf.mu.Unlock()

	if len(rf.LogsCommitCh) == 0 {
		rf.LogsCommitCh <- 1
	}

}

func (rf *Raft) sendHeartBeats(name int, args AppendEnteryArgs) {
	var reply AppendEnteryReply

	//atomic.StoreInt64(&rf.LastMsgTime, time.Now().UnixNano())
	rf.mu.Lock()
	args.PreLogIndex = atomic.LoadInt32(&rf.NextAppliedIndex[name]) - 1
	args.PreLogTerm = rf.Logs[args.PreLogIndex].Term

	appendLoglen := int(rf.LastAppliedIndex - args.PreLogIndex)
	if appendLoglen >= 1 {
		args.Enteries = make([]Entery, appendLoglen)
	}
	for i := 0; int32(i)+args.PreLogIndex+1 <= rf.LastAppliedIndex; i++ {
		args.Enteries[i] = rf.Logs[i+int(args.PreLogIndex)+1]
	}
	rf.mu.Unlock()

	ok := rf.peers[name].Call("Raft.HandleHeartBeats", args, &reply)

	if ok {
		atomic.StoreInt32(&rf.NextAppliedIndex[name], reply.LastAppliedIndex+1)
		if ok && reply.Term > atomic.LoadInt32(&rf.Term) &&
			atomic.LoadInt32(&rf.Role) == Leader {
			rf.mu.Lock()

			fmt.Println("I become follower rf,me", rf.me)
			atomic.StoreInt32(&rf.Role, Follower)
			atomic.StoreInt32(&rf.Term, reply.Term)

			//fmt.Println("EEEEEEEE")
			atomic.StoreInt32(&rf.LastAppliedIndex, atomic.LoadInt32(&rf.CommittedIndex))
			atomic.StoreInt32(&rf.LeaderCommittedIndex, atomic.LoadInt32(&rf.CommittedIndex))

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) BroadCast() {
	//fmt.Println("BroadCast", rf.me)
	var args AppendEnteryArgs

	args.Term = atomic.LoadInt32(&rf.Term)
	args.LeaderCommittedIndex = atomic.LoadInt32(&rf.CommittedIndex)
	atomic.StoreInt64(&rf.LastMsgTime, time.Now().UnixNano())

	for i := 0; i < int(rf.PeersNum); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendHeartBeats(i, args)
	}
	//可以看看有没有别的判空的方法
	if len(rf.LogsCommitCh) < 1 {
		rf.LogsCommitCh <- 1
	}
}
