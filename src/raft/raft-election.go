package raft

import "sync"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate term
	CandidateId  int
	LastLogIndex int // last log index
	LastLogTerm  int // last log term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // current term to update itself
	VoteGranted bool // cnaiddate receive vote
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	// return term, index
	return rf.log[len(rf.log)-1].Term, len(rf.log) - 1
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// deal with request including requests and request reply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	receiverLastLogTerm, receiverLastLogIndex := rf.lastLogTermIndex()

	/*
			Current terms are exchanged
		whenever servers communicate; if one server’s current
		term is smaller than the other’s, then it updates its current
		term to the larger value. If a candidate or leader discovers
		that its term is out of date, it immediately reverts to fol-
		lower state. If a server receives a request with a stale term
		number, it rejects the request.
	*/
	// return flase if request is out of date args.Term < rf.currentTerm
	// return false if args.Term > rf.currentTerm receiver is out of date may happen a split vote,
	// another partition already in next election
	if args.Term < rf.currentTerm {
		// when a vote split happens, ther currentTerm(intilize from beginning) is ahead of this request term
		// reject this term vot and update
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			// become follower and update current term
			rf.currentTerm = args.Term
			// rf.isLeader = false
			rf.changeToFollower()

			rf.votedFor = -1
		}

		// then from the figure 2 or at least up to date log
		// !!!candidate lgo new as receiver'slog, receiver is equal to yourself
		// can use innerfor loop or use another function
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogTerm > receiverLastLogTerm ||
				args.LastLogTerm == receiverLastLogTerm &&
					args.LastLogIndex >= receiverLastLogIndex {
				rf.resetElectionTimer()
				// cahnge the receiver status
				rf.changeToFollower()
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true

			}
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) election() {
	rf.mu.Lock()

	rf.resetElectionTimer()

	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}

	rf.changeToCandidate()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	voteargs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	// grantedCount := 1
	// votedCount := 1
	// gor goroutines
	var wg sync.WaitGroup
	replyCh := make(chan RequestVoteReply, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		// foryourself directly pass or simply add some elction time
		//incase when you send a request your electionmer satart
		// if i==rf.me{rf.electionTimer.Reset(rf.electionTimeout)}// add the condidtion -> so the resetTimer() here wins
		if i == rf.me {
			// rf.resetElectionTimer()
			continue
		} else {
			wg.Add(1)
			// start done in the same time go func needs
			go func(n int) {
				defer wg.Done()
				reply := RequestVoteReply{}
				doneCh := make(chan bool, 1)

				go func() {
					// send multiple requests smae time so only a ok for reply would cause error
					ok := rf.sendRequestVote(n, &voteargs, &reply)
					doneCh <- ok

				}()

				select {
				case ok := <-doneCh:
					if !ok {
						return
					}
					// fmt.Println(n, rf.me, rf.votedFor)
					// fmt.Println(reply.Term, reply.VoteGranted)
					replyCh <- reply
				}
			}(i)
			// start vote process
		}
	}

	go func() {
		wg.Wait()
		close(replyCh)
	}()

	voteResult := 1
	for reply := range replyCh {
		if reply.VoteGranted {
			voteResult++
			if voteResult > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.changeToLeader()
				// fmt.Printf("leader is %v \n",rf.me)
				// fmt.Print("nuber of live",len(rf.peers))
				// fmt.Print("\n")
				rf.mu.Unlock()
				return

			}

		} else if reply.Term > voteargs.Term {
			// check if reply term is the lateset

			// vote is out of date
			rf.mu.Lock()
			rf.changeToFollower()
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			rf.resetElectionTimer()
			return

		}
	}

}
