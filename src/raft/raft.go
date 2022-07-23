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

	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

type Log struct {
	INDEX int
	TERM  int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
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

	// sync usage
	receive_from_leader bool

	// Persistent on all servers
	current_term int
	voted_for    int
	log          []Log

	// Volatile on all servers
	commit_index int
	last_applied int

	// Volatile on leaders
	next_index  []int
	match_index []int
	status      int

	// Convinence constants
	all_server_number int
	quorum_number     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.current_term
	if rf.status == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
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
	TERM         int
	CANDIDATE_ID int
	LAST_LOG     Log // last log in candidate, if no log, term is -1, also index
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TERM         int  // currentTerm, for candidate to update itself
	VOTE_GRANTED bool // true means candidate receive vote
}

type RequestAppendEntryArgs struct {
	TERM           int // leader's term
	LEADER_ID      int // for follower redirect clients
	PREV_LOG_INDEX int
	PREV_LOG_TERM  int
	ENTRIES        []Log // log entries to store
	LEADER_COMMIT  int   // leader's commit_index
}

type RequestAppendEntryReply struct {
	TERM    int  // receiver's current term
	SUCCESS bool // true if contain matching prev log

}

// func (rf *Raft) logInfo(str string, args ...interface{}) {
// 	tmp := "Server[%d]"
// 	fstr := strings.Join(tmp, str)
// 	log.Printf(fstr, rf.me, args...)
// }

func (rf *Raft) sendAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendoneAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	ok := rf.sendAppendEntry(server, args, reply)
	for !ok {
		interval := 10
		time.Sleep(time.Duration(interval) * time.Millisecond)
		ok = rf.sendAppendEntry(server, args, reply)
	}

	return
}

func (rf *Raft) sendOneRoundHeartBeat() {
	i := 0
	args := &RequestAppendEntryArgs{}
	reply := &RequestAppendEntryReply{}

	rf.mu.Lock()
	if rf.status != LEADER {
		rf.mu.Unlock()
		return
	}
	log.Printf("Server[%d] start new round beat", rf.me)
	args.TERM = rf.current_term
	args.LEADER_ID = rf.me
	args.LEADER_COMMIT = rf.commit_index
	rf.mu.Unlock()

	for i = 0; i < rf.all_server_number; i++ {
		// TODO: reply will cause data race, fix it later
		if i == rf.me {
			continue
		}
		go rf.sendoneAppendEntry(i, args, reply)
		log.Printf("Server[%d] send heart beat to server[%d]", rf.me, i)
	}
}

func (rf *Raft) sendHeartBeat() {
	interval := 100
	for {
		time.Sleep(time.Duration(interval) * time.Millisecond)
		rf.mu.Lock()
		if rf.status != LEADER {
			log.Printf("Server[%d] quit send heart beat, no longer leader", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.sendOneRoundHeartBeat()
	}
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Server[%d] receive heart beat from server[%d]", rf.me, args.LEADER_ID)

	if args.TERM < rf.current_term {
		log.Printf("Server[%d] reject heart beat from server[%d]", rf.me, args.LEADER_ID)
		reply.SUCCESS = false
		return
	}
	log.Printf("Server[%d] accept heart beat from server[%d]", rf.me, args.LEADER_ID)

	prev_status := rf.status

	rf.status = FOLLOWER
	rf.receive_from_leader = true
	rf.current_term = args.TERM
	if prev_status != rf.status {
		log.Printf("Server[%d] become follower", rf.me)
	}

	// TODO: other thing to append entries

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Server[%d] got vote request from %d", rf.me, args.CANDIDATE_ID)

	reply.VOTE_GRANTED = false
	reply.TERM = rf.current_term

	// I have newer term
	if rf.current_term > args.TERM {
		log.Printf("Server[%d] reject vote request from %d, because term", rf.me, args.CANDIDATE_ID)
		return
	}

	// I have voted for other server
	if rf.voted_for != -1 && rf.voted_for != args.CANDIDATE_ID {
		log.Printf("Server[%d] reject vote request from %d, because have voted other server", rf.me, args.CANDIDATE_ID)
		return
	}

	// I have newer log
	if len(rf.log) > 0 {
		my_latest_log := &rf.log[len(rf.log)-1]

		if my_latest_log.TERM != args.LAST_LOG.TERM {
			if my_latest_log.TERM > args.LAST_LOG.TERM {
				log.Printf("Server[%d] reject vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
				return
			}
		}

		if my_latest_log.TERM == args.LAST_LOG.TERM {
			if my_latest_log.INDEX > args.LAST_LOG.INDEX {
				log.Printf("Server[%d] reject vote request from %d, because have newer log, 2", rf.me, args.CANDIDATE_ID)
				return
			}
		}
	}

	// I can voted candidate now
	// TODO: is there other things to do?
	log.Printf("Server[%d] vote for Server[%d]", rf.me, args.CANDIDATE_ID)
	reply.VOTE_GRANTED = true
	rf.voted_for = args.CANDIDATE_ID
	rf.current_term = args.TERM
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
	log.Printf("Server[%d] send vote request to server[%d]", args.CANDIDATE_ID, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) requestOneServerVote(index int, ans *chan RequestVoteReply) {
	// args are the same, so it actually can use only one variable, but I'm lazy
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	rf.mu.Lock()
	args.TERM = rf.current_term
	args.CANDIDATE_ID = rf.me
	if len(rf.log) == 0 {
		args.LAST_LOG = Log{
			TERM:  -1,
			INDEX: -1,
		}
	} else {
		// NOTE: what is = meaning?
		// update: it create a copy, nothing to worry about
		args.LAST_LOG = rf.log[len(rf.log)-1]
	}
	rf.mu.Unlock()

	for {
		ok := false

		rf.mu.Lock()
		if rf.status == CANDIDATE {
			rf.mu.Unlock()
			if index == rf.me {
				rf.RequestVote(args, reply)
				ok = true
			} else {
				ok = rf.sendRequestVote(index, args, reply)
			}
		} else {
			rf.mu.Unlock()
			return
		}

		if ok {
			rf.mu.Lock()
			if rf.status == CANDIDATE {
				*ans <- *reply
			}
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) newVote() {
	rf.mu.Lock()
	rf.current_term += 1
	rf.status = CANDIDATE
	rf.voted_for = -1
	rf.mu.Unlock()

	log.Printf("Server[%d] new vote", rf.me)

	got_tickets := 0
	reply := make(chan RequestVoteReply, rf.all_server_number)
	for i := 0; i < rf.all_server_number; i++ {
		// NOTE: should very careful about goroutine leak
		go rf.requestOneServerVote(i, &reply)
	}

	for {
		vote_reply := <-reply
		log.Printf("Server[%d] got vote reply, granted is %t", rf.me, vote_reply.VOTE_GRANTED)
		if !vote_reply.VOTE_GRANTED {
			continue
		}

		got_tickets += 1

		// current status is not candidate anymore
		rf.mu.Lock()
		if rf.status != CANDIDATE {
			rf.mu.Unlock()
			return
		}

		// tickets not enough
		if got_tickets < rf.quorum_number {
			rf.mu.Unlock()
			continue
		} else {
			// tickets enough
			rf.status = LEADER
			rf.mu.Unlock()

			log.Printf("Server[%d] win the vote", rf.me)
			rf.sendOneRoundHeartBeat()
			go rf.sendHeartBeat()

			return
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

// The Ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		duration := rand.Intn(300) + 300
		log.Printf("Server[%d] ticker: wait time is %d(ms)", rf.me, duration)

		time.Sleep(time.Duration(duration) * time.Millisecond)

		rf.mu.Lock()
		log.Printf("Server[%d] ticker: receive from leader is %t", rf.me, rf.receive_from_leader)
		if !rf.receive_from_leader && rf.status != LEADER {
			rf.mu.Unlock()
			go rf.newVote()
		} else {
			rf.receive_from_leader = false
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) mainLoop() {
	switch rf.status {
	case FOLLOWER:
		// stop voting
	case CANDIDATE:
		//
	case LEADER:
		// send heart beat periodically
	}
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
	file, err := os.OpenFile("raft.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	rf.voted_for = -1
	rf.status = FOLLOWER
	rf.all_server_number = len(rf.peers)
	rf.quorum_number = (rf.all_server_number + 1) / 2

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
