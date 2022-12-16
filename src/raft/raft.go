package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// OWN PROPERTY difference for timeout and timer

	// state a Raft server must maintain.
	currentTerm int        //default as 0, increase monotonicayyl like a counter of status as a GLOABL COUNTER
	votedFor    int        // candidate id??? canbe null ??
	log         []LogEntry //log entries; command + term ???!!! need to define log first and then applied

	commitIndex int //index of highest log entry;0 latest one
	lastApplied int // index of hiest log entry to status machine;0
	// intilization by first call after election
	nextIndex  []int // index og next log entry to send to server
	matchIndex []int //index of hiest log entry to replicated on server ???meaning

	role       Role

	applyCh chan ApplyMsg

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	killCh        chan struct{}
	notifyApplyCh chan struct{}
	notifySnapCh  chan ApplyMsg

}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const (
	ElectionTimeout  = time.Millisecond * 150
	HeartBeatTimeout = time.Millisecond * 100
)

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func randomizeElectionTimeout() time.Duration {
	return ElectionTimeout + (time.Duration(rand.Int63()) % ElectionTimeout)
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomizeElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}

func (rf *Raft) changeToLeader() {
	rf.role = Leader

	_, lastLogIndex := rf.lastLogTermIndex()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = lastLogIndex
	DPrintf("raft %d become leader:%v", rf.me, rf)
}

func (rf *Raft) changeToCandidate() {
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.currentTerm += 1
	DPrintf("raft %d become candidate:%v", rf.me, rf)
}

func (rf *Raft) changeToFollower() {
	rf.role = Follower
	DPrintf("raft %d become follower:%v", rf.me, rf)
}
func (rf *Raft) ticker() {

	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.electionTimer.C:
			go rf.election()
		}
	}
}
func (rf *Raft) heartbeatTicker() {
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.heartbeatTimer.C:
			rf.resetHeartbeatTimer()
			for index := range rf.peers {
				if rf.me == index {
					continue
				} else {
					go rf.heartbeat(index)
				}
			}
		}
	}

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.killCh = make(chan struct{})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.changeToFollower()

	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(randomizeElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatTimeout)

	rf.notifyApplyCh = make(chan struct{}, 100)


	// rf.lastApplied = rf.lastLogIndex

	go rf.ticker()

	go rf.heartbeatTicker()

	go rf.applyLog()

	return rf
}
