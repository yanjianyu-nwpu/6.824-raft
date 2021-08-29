package raft

import (
	"sync/atomic"
)

func (rf *Raft) LogCommittedCircle() {
	for rf.killed() == false {
		<-rf.LogsCommitCh

		for atomic.LoadInt32(&rf.CommittedIndex) < atomic.LoadInt32(&rf.LastAppliedIndex) {
			rf.mu.Lock()
			nextCommittedIndex := atomic.LoadInt32(&rf.CommittedIndex) + 1

			if atomic.LoadInt32(&rf.Role) == Leader {

				if nextCommittedIndex > atomic.LoadInt32(&rf.LastAppliedIndex) {
					break
				}
				ji := 1
				for i := 0; i < int(rf.PeersNum); i++ {
					if i == rf.me {
						continue
					}
					if atomic.LoadInt32(&rf.NextAppliedIndex[i]) > nextCommittedIndex {
						ji++
					}
				}
				if ji >= int(rf.PeersNum/2)+1 {
					AM := ApplyMsg{}
					AM.CommandValid = true
					AM.Command = rf.Logs[int(nextCommittedIndex)].Command
					AM.CommandIndex = int(nextCommittedIndex)

					//fmt.Println("Leader Commit", "Rf.me", rf.me, "INdex", AM.CommandIndex,
					//	"Command", AM.Command, "Term", rf.Term)

					rf.applyCh <- AM
					atomic.AddInt32(&rf.CommittedIndex, 1)

					rf.persist()
				}
				rf.mu.Unlock()
				break
			} else {
				if nextCommittedIndex <= atomic.LoadInt32(&rf.LeaderCommittedIndex) &&
					nextCommittedIndex <= atomic.LoadInt32(&rf.LeaderCommittedIndex) {

					AM := ApplyMsg{}
					AM.CommandValid = true
					AM.Command = rf.Logs[int(nextCommittedIndex)].Command
					AM.CommandIndex = int(nextCommittedIndex)

					//fmt.Println("Follower Commit", "Rf.me", rf.me, "INdex", AM.CommandIndex,
					//	"Command", AM.Command, "Term", atomic.LoadInt32(&rf.Term))
					rf.applyCh <- AM
					atomic.AddInt32(&rf.CommittedIndex, 1)
					rf.persist()
				}
				rf.mu.Unlock()
				break
			}

			//rf.mu.Unlock()
		}
	}
}
