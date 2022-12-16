package raft

/*
needs to be updated:
	after sending
	commitIndex int //index of highest log entry;0 latest one
	lastApplied int // index of hiest log entry to status machine;0

	in sending
	nextIndex  []int // index og next log entry to send to server sender
	matchIndex []int //index of highest log entry to replicated on server ???meaning receiver
*/

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int // redirect to leader id, incase folower to send to client
	PrevLogIndex int //index of log precesing nre one-> the new one precessor
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	LogTerm  int
	LogIndex int // from reply to be received the lastindex of log

	Abandon bool
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader
	_, lastIndex := rf.lastLogTermIndex()

	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = lastIndex + 1

		// in case of rending when sending
		rf.resetHeartbeatTimer()
		for index := range rf.peers {
			if rf.me == index {
				continue
			} else {
				// send heartbeat to each peer
				go rf.heartbeat(index)
			}
		}
	}

	return lastIndex + 1, term, isLeader
}

func (rf *Raft) heartbeat(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	// next index in request(the leader knows) to send
	nextIndex := rf.nextIndex[server]
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		// first check leader and term if not mach return false
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		//////////////////// intilization
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			Entries:      rf.log[nextIndex:],
			PrevLogTerm:  rf.log[nextIndex-1].Term,
			PrevLogIndex: nextIndex - 1,
		}

		rf.mu.Unlock()

		///////////////////////////send log///////////////////////////////
		//send log
		// the sender and receiver cannot process data in the same time

		replyCh := make(chan AppendEntriesReply, 1)
		go func() {
			// args.Entries
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, &args, &reply)
			DPrintf("raft %d send to raft %d with %v, then get reply %v", rf.me, server, args, reply)

			if ok {
				replyCh <- reply
			}
		}()

		///////////////////////////////deal with reply and conflict //////////////////
		select {
		case reply := <-replyCh:

			// frist check term match
			if reply.Term > rf.currentTerm {
				// me is out ot date
				// direcly update my term and be follower
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.changeToFollower()
				rf.votedFor = -1
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			} else {
				rf.mu.Lock()
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				if !reply.Success {

					// if reply failed , inorder to maintain a high level of coherency
					// deal with conflict and update index if success
					// the protocol:The leader handles inconsistencies by forcing the followers’ logs to duplicate its own
					//If a follower’s log is inconsistent with the leader’s, the AppendEntries consis- tency check will fail in the next AppendEntries RPC
					// if log stored is more latest, aka request is expired, update the nextIndex

					if rf.log[reply.LogIndex].Term == reply.LogTerm {
						nextIndex = reply.LogIndex + 1
					} else {
						nextIndex = reply.LogIndex
					}

				} else {

					rf.nextIndex[server] = nextIndex + len(args.Entries)

					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

					if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
						rf.updateCommitIndex()
					}
					// DPrintf('reply success')

					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				continue
			}

		}

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.LogIndex = lastLogIndex // to receive the log index prev log length
	reply.LogTerm = lastLogTerm
	reply.Abandon = false
	rf.resetElectionTimer()

	////////// check term

	//Reply false if term < currentTerm (§5.1
	if rf.currentTerm < args.Term {
		// itself out of date so update and also continue with append entries
		rf.changeToFollower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
	} else if rf.currentTerm > args.Term {
		// reject and notify that fake leader
		reply.Success = false
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(args.Entries) > 0 && (lastLogTerm > args.Entries[len(args.Entries)-1].Term || (lastLogTerm == args.Entries[len(args.Entries)-1].Term && lastLogIndex > args.PrevLogIndex+len(args.Entries))) {
		reply.Abandon = true
		return
	}

	if len(args.Entries) == 0 && (lastLogTerm > args.PrevLogTerm || (lastLogTerm == args.PrevLogTerm && lastLogIndex > args.PrevLogIndex)) {
		reply.Abandon = true
		return
	}
	////////// check index

	if args.PrevLogIndex > lastLogIndex {
		// If an existing entry conflicts with a new one (different index) and return conflict index of reply
		reply.Success = false
		reply.LogIndex = lastLogIndex
		reply.LogTerm = lastLogTerm
	} else {
		//If an existing entry conflicts with a new one (  same terms), add new log
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		} else {
			//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
			reply.Success = false
			idx := args.PrevLogIndex
			for idx > rf.commitIndex && rf.log[idx].Term == rf.log[args.PrevLogIndex].Term {
				idx -= 1
			}
			reply.LogIndex = idx
			reply.LogTerm = rf.log[idx].Term
		}
	}

	if reply.Success {
		// modify commit of receiver
		if rf.commitIndex < args.LeaderCommit {
			//set commitIndex = min(leaderCommit, index of last new entry)
			// lack of certain to be commited in receiver
			if args.LeaderCommit > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.notifyApplyCh <- struct{}{}
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndex() {
	hasCommit := false
	for i := rf.commitIndex + 1; i <= len(rf.log)-1; i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if hasCommit {
		// notify appply function
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) applyLog() {
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.notifyApplyCh:
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex {
				msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msgs = append(msgs, ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					})
				}
				rf.lastApplied = msgs[len(msgs)-1].CommandIndex
				rf.mu.Unlock()
				for _, msg := range msgs {
					rf.applyCh <- msg
				}
				continue
			}
			rf.mu.Unlock()
		}
	}
}
