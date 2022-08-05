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
	"runtime"
	"sort"
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
	INDEX   int
	TERM    int
	COMMAND interface{}
}

const (
	FOLLOWER = iota
	PRECANDIDATE
	CANDIDATE
	LEADER
)

type ServerCommitIndex struct {
	server       int
	commit_index []int
}

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
	leader_id           int

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

	// Convinence chanel
	recently_commit   chan ServerCommitIndex
	append_entry_chan []chan *RequestAppendEntryArgs
	apply_ch          chan ApplyMsg

	// Convinence constants
	all_server_number int
	quorum_number     int
	timeout_const_ms  int
	timeout_rand_ms   int
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
	if rf.statusIs(LEADER) {
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
	TERM           int
	CANDIDATE_ID   int
	PREV_LOG_INDEX int
	PREV_LOG_TERM  int
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
	TERM           int   // leader's term
	LEADER_ID      int   // for follower redirect clients
	ENTRIES        []Log // log entries to store
	LEADER_COMMIT  int   // leader's commit_index
	PREV_LOG_INDEX int
	PREV_LOG_TERM  int
}

type RequestAppendEntryReply struct {
	TERM    int  // receiver's current term
	SUCCESS bool // true if contain matching prev log
}

func (rf *Raft) sendAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendOneAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	if len(args.ENTRIES) == 0 {
		log.Printf("Server[%d] send new heartbeat to %d", rf.me, server)
	}
	ok := rf.sendAppendEntry(server, args, reply)
	failed_times := 0
	interval := 10
	for !ok {
		if failed_times > 10 {
			return
		}
		time.Sleep(time.Duration(interval) * time.Millisecond)
		rf.mu.Lock()
		if rf.current_term > args.TERM {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok = rf.sendAppendEntry(server, args, reply)
		failed_times++
		log.Print("Server[", rf.me, "] send append entry failed times ", failed_times, " to server ", server)
		if len(args.ENTRIES) == 0 {
			log.Printf("Server[%d] send new heartbeat to %d", rf.me, server)
		}
	}
	log.Printf("Server[%d] send new heartbeat to %d DONE", rf.me, server)

	return
}

func (rf *Raft) leaderUpdateCommitIndex(current_term int) {
	commited := make([]int, 0)
	for {
		new_commit := <-rf.recently_commit
		rf.mu.Lock()
		log.Printf("get new commit")
		log.Print(new_commit)
		if current_term != rf.current_term {
			log.Printf("Server[%d] quit leader update commit, term change", rf.me)
			rf.mu.Unlock()
			return
		}

		for _, ele := range new_commit.commit_index {
			commit_server_num := 0

			if ele <= rf.commit_index {
				continue
			}

			for j := 0; j < rf.all_server_number; j++ {
				// FIXME: update next_index! and prevent index n+1 can not commit because n did not count
				if rf.next_index[j] > ele {
					commit_server_num += 1
				}
			}

			if commit_server_num >= rf.quorum_number {
				commited = append(commited, ele)
			}
		}

		sort.Slice(commited, func(i, j int) bool {
			return commited[i] < commited[j]
		})
		log.Printf("commited is ")
		log.Print(commited)

		new_commited := make([]int, 0)
		for _, ele := range commited {
			if ele == rf.commit_index+1 {
				rf.updateCommitIndex(ele)
			} else {
				new_commited = append(new_commited, ele)
			}
		}
		commited = new_commited

		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex(new_commit_index int) {
	if new_commit_index <= rf.commit_index {
		return
	}

	for i := rf.commit_index + 1; i <= new_commit_index; i++ {
		if i > len(rf.log) {
			break
		}
		tmp := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i-1].COMMAND,
			CommandIndex: rf.log[i-1].INDEX,
		}
		rf.apply_ch <- tmp
		rf.commit_index = i
		log.Printf("Server[%d] commit index %d", rf.me, tmp.CommandIndex)
	}

}

// use this function with lock
func (rf *Raft) sendOneRoundHeartBeat() {
	i := 0
	args := &RequestAppendEntryArgs{}
	reply := make([]RequestAppendEntryReply, rf.all_server_number)
	if !rf.statusIs(LEADER) {
		return
	}

	args.TERM = rf.current_term
	args.LEADER_ID = rf.me
	args.LEADER_COMMIT = rf.commit_index

	for i = 0; i < rf.all_server_number; i++ {
		// don't send heart beat to myself
		if i == args.LEADER_ID {
			continue
		}
		go rf.sendOneAppendEntry(i, args, &reply[i])
	}
}

func (rf *Raft) sendHeartBeat() {
	interval := 100 // ms
	for {
		time.Sleep(time.Duration(interval) * time.Millisecond)
		rf.mu.Lock()
		if !rf.statusIs(LEADER) {
			rf.mu.Unlock()
			log.Printf("Server[%d] quit send heart beat, no longer leader", rf.me)
			return
		}

		rf.sendOneRoundHeartBeat()
		rf.mu.Unlock()
	}
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("Server[%d] receive Append Entry RPC from server[%d]", rf.me, args.LEADER_ID)
	// log.Print(args)

	if args.TERM < rf.current_term {
		log.Printf("Server[%d] reject Append Entry RPC from server[%d]", rf.me, args.LEADER_ID)
		reply.SUCCESS = false
		reply.TERM = rf.current_term
		return
	}

	if !rf.statusIs(FOLLOWER) || rf.leader_id != args.LEADER_ID {
		log.Printf("Server[%d] accept new heart beat from server[%d], at term %d", rf.me, args.LEADER_ID, args.TERM)
	}

	if len(args.ENTRIES) == 0 {
		log.Printf("Server[%d] receive new heart beat from server[%d], at term %d", rf.me, args.LEADER_ID, args.TERM)
	}

	if args.LEADER_ID != rf.me {
		rf.becomeFollower(args.TERM, args.LEADER_ID)
	}

	// log did not match
	// TODO: may need find a way to speed up this
	if len(rf.log) > 0 && len(args.ENTRIES) > 0 {
		matched := false
		for i := len(rf.log) - 1; i >= 0; i-- {
			iter_log := &rf.log[i]
			// log match
			if args.PREV_LOG_INDEX == iter_log.INDEX && args.PREV_LOG_TERM == iter_log.TERM {
				matched = true
				// match happen before latest log
				// TODO: find how dismatch is this
				if i < len(rf.log)-1 {
					rf.log = rf.log[:i+1]
				}
				break
			}
		}
		if !matched {
			reply.SUCCESS = false
			log.Printf("Server[%d] Append Entry failed because PREV_LOG_INDEX %d not matched", rf.me, args.PREV_LOG_INDEX)
			return
		}
	}

	// rpc call success
	reply.SUCCESS = true

	for i, j := 0, len(args.ENTRIES)-1; i < j; i, j = i+1, j-1 {
		args.ENTRIES[i], args.ENTRIES[j] = args.ENTRIES[j], args.ENTRIES[i]
	}

	rf.log = append(rf.log, args.ENTRIES...)

	if len(args.ENTRIES) > 0 {
		log.Print("Server[", rf.me, "] going to commit args:", args)
		log.Print("Server[", rf.me, "] have log", rf.log)
	}

	rf.updateCommitIndex(args.LEADER_COMMIT)

	return
}

func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.TERM = rf.current_term
	reply.VOTE_GRANTED = false
	log.Printf("Server[%d] got pre vote request from Server[%d]", rf.me, args.CANDIDATE_ID)

	// caller term less than us
	if args.TERM < rf.current_term {
		log.Printf("Server[%d] reject pre vote request from Server[%d], term too low", rf.me, args.CANDIDATE_ID)
		return
	}

	// last AppendEntries call was received less than election timeout ago
	if rf.receive_from_leader {
		log.Printf("Server[%d] reject pre vote request from Server[%d], already have leader [%d]", rf.me, args.CANDIDATE_ID, rf.leader_id)
		return
	}

	// I have newer log
	if len(rf.log) > 0 {
		my_latest_log := &rf.log[len(rf.log)-1]

		if (my_latest_log.TERM != args.PREV_LOG_TERM) && (my_latest_log.TERM > args.PREV_LOG_TERM) {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
			return
		}

		if (my_latest_log.TERM == args.PREV_LOG_TERM) && (my_latest_log.INDEX > args.PREV_LOG_INDEX) {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log, 2nd case", rf.me, args.CANDIDATE_ID)
			return
		}
	}

	log.Printf("Server[%d] granted pre vote request from Server[%d]", rf.me, args.CANDIDATE_ID)

	reply.VOTE_GRANTED = true
	return
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
		log.Printf("Server[%d] reject vote request from %d, because its term %d lower than current term %d",
			rf.me, args.CANDIDATE_ID, args.TERM, rf.current_term)
		return
	}

	// caller term greater than us
	if rf.current_term < args.TERM {
		// NOTE: it would not cause mutilply leaders?
		// update: of course not! 1 term, 1 server, 1 ticket, fair enough.
		log.Printf("Server[%d] request vote but they have new term ", rf.me)
		rf.becomeFollower(args.TERM, -1)
	}

	// I have voted for other server
	if rf.voted_for != -1 && rf.voted_for != args.CANDIDATE_ID {
		log.Printf("Server[%d] reject vote request from %d, because have voted server[%d], at term %d",
			rf.me, args.CANDIDATE_ID, rf.voted_for, rf.current_term)
		return
	}

	// I have newer log
	if len(rf.log) > 0 {
		my_latest_log := &rf.log[len(rf.log)-1]

		if (my_latest_log.TERM != args.PREV_LOG_TERM) && my_latest_log.TERM > args.PREV_LOG_TERM {
			log.Printf("Server[%d] reject vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
			return
		}

		if (my_latest_log.TERM == args.PREV_LOG_TERM) && (my_latest_log.INDEX > args.PREV_LOG_INDEX) {
			log.Printf("Server[%d] reject vote request from %d, because have newer log, 2nd case", rf.me, args.CANDIDATE_ID)
			return
		}
	}

	// I can voted candidate now
	log.Printf("Server[%d] vote for Server[%d], at term %d", rf.me, args.CANDIDATE_ID, rf.current_term)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendPreVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestPreVote", args, reply)
	return ok
}

func (rf *Raft) requestOneServerVote(index int, ans chan RequestVoteReply, this_round_term int) {
	// args are the same, so it actually can use only one variable, but I'm lazy
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	rf.mu.Lock()
	args.TERM = this_round_term
	args.CANDIDATE_ID = rf.me
	if len(rf.log) == 0 {
		args.PREV_LOG_TERM = -1
		args.PREV_LOG_INDEX = -1
	} else {
		args.PREV_LOG_INDEX = rf.log[len(rf.log)-1].INDEX
		args.PREV_LOG_TERM = rf.log[len(rf.log)-1].TERM
	}
	rf.mu.Unlock()
	failed_times := 0

	for {
		ok := false
		if failed_times > 10 {
			return
		}

		rf.mu.Lock()
		if rf.current_term != args.TERM {
			rf.mu.Unlock()
			log.Printf("Server[%d] quit requestOneServerVote for Server[%d], because new term", rf.me, index)
			return
		}
		if !rf.statusIs(CANDIDATE) {
			rf.mu.Unlock()
			log.Printf("Server[%d] quit requestOneServerVote for Server[%d], because is not CANDIDATE", rf.me, index)
			return
		}
		rf.mu.Unlock()
		ok = rf.sendRequestVote(index, args, reply)

		if !ok {
			internal := 10
			failed_times++
			time.Sleep(time.Duration(internal) * time.Millisecond)
			continue
		}

		log.Printf("Server[%d] send vote request to server[%d]", rf.me, index)
		rf.mu.Lock()
		if rf.statusIs(CANDIDATE) {
			ans <- *reply
		}
		rf.mu.Unlock()
		return

	}
}

func (rf *Raft) requestOneServerPreVote(index int, ans chan RequestVoteReply, this_round_term int) {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	rf.mu.Lock()
	args.TERM = this_round_term + 1
	args.CANDIDATE_ID = rf.me

	if len(rf.log) == 0 {
		args.PREV_LOG_TERM = -1
		args.PREV_LOG_INDEX = -1
	} else {
		args.PREV_LOG_TERM = rf.log[len(rf.log)-1].TERM
		args.PREV_LOG_INDEX = rf.log[len(rf.log)-1].INDEX
	}
	rf.mu.Unlock()
	log.Printf("Server[%d] start requestOneServerPreVote", rf.me)
	defer log.Printf("Server[%d] quit requestOneServerPreVote", rf.me)
	failed_times := 0

	for {
		ok := false

		if failed_times > 10 {
			return
		}

		rf.mu.Lock()
		if rf.current_term != this_round_term {
			rf.mu.Unlock()
			log.Printf("Server[%d] quit requestOneServerPreVote for Server[%d], because new term", rf.me, index)
			return
		}

		if rf.status != PRECANDIDATE {
			rf.mu.Unlock()
			log.Printf("Server[%d] quit requestOneServerPreVote for Server[%d], because is not PRECANDIDATE", rf.me, index)
			return
		}
		rf.mu.Unlock()
		ok = rf.sendPreVote(index, args, reply)

		if !ok {
			internal := 10
			failed_times++
			time.Sleep(time.Duration(internal) * time.Millisecond)
			continue
		}

		log.Printf("Server[%d] send pre vote request to server[%d]", rf.me, index)
		rf.mu.Lock()
		if rf.status == PRECANDIDATE {
			ans <- *reply
		}
		rf.mu.Unlock()
		return

	}
}

func (rf *Raft) newVote(this_round_term int) {
	rf.mu.Lock()
	if this_round_term != rf.current_term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	log.Printf("Server[%d] new vote", rf.me)

	got_tickets := 0
	reply := make(chan RequestVoteReply, rf.all_server_number)
	for i := 0; i < rf.all_server_number; i++ {
		go rf.requestOneServerVote(i, reply, this_round_term)
	}

	timeout_ms := rf.timeout_const_ms + rf.timeout_rand_ms

	for {
		select {
		case vote_reply := <-reply:
			log.Printf("Server[%d] got vote reply, granted is %t", rf.me, vote_reply.VOTE_GRANTED)
			if !vote_reply.VOTE_GRANTED {
				continue
			}

			got_tickets += 1
			log.Printf("Server[%d] got vote tickets number is %d", rf.me, got_tickets)

			// current status is not candidate anymore
			rf.mu.Lock()
			if rf.status != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			// term is new term
			if this_round_term != rf.current_term {
				rf.mu.Unlock()
				log.Printf("Server[%d] quit last vote, because term %d is old", rf.me, this_round_term)
				return
			}

			// tickets not enough
			if got_tickets < rf.quorum_number {
				rf.mu.Unlock()
				continue
			}
			// tickets enough
			rf.becomeLeader()
			rf.mu.Unlock()

			return

		case <-time.After(time.Duration(timeout_ms) * time.Millisecond):
			log.Printf("Server[%d] did not finish vote in time", rf.me)
			return

		default:
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}

	// code unreachable

}

func (rf *Raft) newPreVote(this_round_term int) bool {
	rf.mu.Lock()
	if this_round_term != rf.current_term {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	log.Printf("Server[%d] start new pre vote", rf.me)
	defer log.Printf("Server[%d] quit pre vote", rf.me)
	got_tickets := 0
	got_reply := 0
	reply := make(chan RequestVoteReply, rf.all_server_number)
	for i := 0; i < rf.all_server_number; i++ {
		go rf.requestOneServerPreVote(i, reply, this_round_term)
	}

	timeout_ms := rf.timeout_const_ms + rf.timeout_rand_ms

	for {
		select {
		case pre_vote_reply := <-reply:
			got_reply += 1
			log.Printf("Server[%d] got pre vote reply, granted is %t", rf.me, pre_vote_reply.VOTE_GRANTED)
			if !pre_vote_reply.VOTE_GRANTED {
				continue
			}

			got_tickets += 1
			log.Printf("Server[%d] got pre vote tickets number is %d", rf.me, got_tickets)

			// current status is not pre-candidate anymore
			rf.mu.Lock()
			if rf.status != PRECANDIDATE {
				rf.mu.Unlock()
				log.Printf("Server[%d] quit pre vote, not PRECANDIDATE anymore", rf.me)
				return false
			}

			// term is new term
			if rf.current_term != this_round_term {
				rf.status = FOLLOWER
				rf.mu.Unlock()
				log.Printf("Server[%d] quit pre vote, not term %d anymore", rf.me, this_round_term)
				return false
			}

			// tickets not enough
			if got_tickets < rf.quorum_number {
				if got_reply == rf.all_server_number {
					rf.status = FOLLOWER
					rf.mu.Unlock()
					log.Printf("Server[%d] lost the pre vote", rf.me)
					return false
				}
				rf.mu.Unlock()
				continue
			}

			// tickets enough
			rf.becomeCandidate(this_round_term + 1)
			log.Printf("Server[%d] win the pre vote", rf.me)

			this_round_term = rf.current_term
			rf.mu.Unlock()

			return true

		case <-time.After(time.Duration(timeout_ms) * time.Millisecond):
			log.Printf("Server[%d] quit pre-vote goroutine", rf.me)
			return false

		default:
			time.Sleep(time.Duration(10) * time.Millisecond)

		}
	}

	//code unreachable

}

func (rf *Raft) __successAppend(server int, this_round_term int,
	args *RequestAppendEntryArgs) {
	commit_index := make([]int, len(args.ENTRIES))
	for i := 0; i < len(args.ENTRIES); i++ {
		commit_index[i] = args.ENTRIES[i].INDEX
	}

	if server != rf.me {
		rf.mu.Lock()
		rf.next_index[server] = args.PREV_LOG_INDEX + len(args.ENTRIES) + 1
		rf.mu.Unlock()
	}
	rf.recently_commit <- ServerCommitIndex{
		server:       server,
		commit_index: commit_index,
	}
}

func (rf *Raft) handleAppendEntryForOneServer(server int, this_round_term int) {
	defer log.Print("Server[", server, "] quit handleAppendEntryForOneServer")

	for {
		args := <-rf.append_entry_chan[server]
		log.Print("Server[", rf.me, "] running handleAppendEntryForOneServer for ", server)
		rf.mu.Lock()
		if rf.current_term != this_round_term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if server == rf.me {
			rf.__successAppend(server, this_round_term, args)
			continue
		}

		reply := &RequestAppendEntryReply{}

		failed_times := 0
		for reply.SUCCESS == false {
			rf.sendOneAppendEntry(server, args, reply)
			if reply.SUCCESS {
				rf.__successAppend(server, this_round_term, args)
				continue
			}
			failed_times++
			log.Print("Server[", server, "] failed time: ", failed_times, ", args is ", args)

			// reply is false
			rf.mu.Lock()

			// new term case 1
			if this_round_term != rf.current_term {
				rf.mu.Unlock()
				return
			}

			// new term case 2, know from peer
			if reply.TERM > rf.current_term {
				rf.mu.Unlock()
				return
			}

			// log not match
			log.Print("Server[", server, "] log dismatch, args is ", args)
			rf.match_index[server] -= 1

			old_prev_log := rf.log[args.PREV_LOG_INDEX-1]
			new_prev_log := &rf.log[args.PREV_LOG_INDEX-2]
			args.PREV_LOG_TERM = new_prev_log.TERM
			args.PREV_LOG_INDEX = new_prev_log.INDEX
			args.ENTRIES = append(args.ENTRIES, old_prev_log)
			log.Print("Server[", server, "] log dismatch, new args is ", args)

			rf.mu.Unlock()
		} // end reply false for
	}

}

func (rf *Raft) newRoundAppend(command interface{}, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := make([]RequestAppendEntryArgs, rf.all_server_number)
	this_round_term := rf.current_term

	new_log := Log{
		INDEX:   index,
		TERM:    this_round_term,
		COMMAND: command,
	}

	logs := []Log{new_log}

	for i := 0; i < rf.all_server_number; i++ {
		args[i].TERM = this_round_term
		args[i].LEADER_ID = rf.me
		args[i].LEADER_COMMIT = rf.commit_index
		args[i].ENTRIES = logs
		prev_log_index := index - 1
		args[i].PREV_LOG_INDEX = prev_log_index
		if prev_log_index > 0 {
			args[i].PREV_LOG_TERM = rf.log[prev_log_index-1].TERM
		}

		rf.append_entry_chan[i] <- &args[i]
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

	// is not leader
	if !rf.statusIs(LEADER) {
		return index, term, false
	}
	term = rf.current_term
	isLeader = true
	index = len(rf.log) + 1
	rf.next_index[rf.me] = index + 1
	new_log := Log{
		INDEX:   index,
		TERM:    rf.current_term,
		COMMAND: command,
	}
	rf.log = append(rf.log, new_log)
	go rf.newRoundAppend(command, index)

	log.Print("Server[", rf.me, "] Start accept new log:", new_log)

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

// use this function when hold the lock
func (rf *Raft) statusIs(status int) bool {
	return rf.status == status
}

// use this function when hold the lock
func (rf *Raft) becomePreCandidate() {
	rf.leader_id = -1
	rf.receive_from_leader = false
	rf.status = PRECANDIDATE
	log.Printf("Server[%d] become pre-candidate", rf.me)
	go rf.newPreVote(rf.current_term)
}

// use this function when hold the lock
func (rf *Raft) becomeCandidate(new_term int) {
	if new_term <= rf.current_term {
		log.Printf("Server[%d] Error: becomeCandidate in same term", rf.me)
		return
	}
	rf.voted_for = -1
	rf.current_term = new_term
	rf.status = CANDIDATE
	log.Printf("Server[%d] become candidate", rf.me)
	go rf.newVote(rf.current_term)
}

// use this function when hold the lock
func (rf *Raft) becomeFollower(new_term int, new_leader int) {
	if new_leader == rf.me {
		log.Printf("Server[%d] become follower by itself, impossble", rf.me)
		return
	}
	if rf.status != FOLLOWER {
		log.Printf("Server[%d] become follower", rf.me)
	}
	rf.status = FOLLOWER

	if rf.current_term < new_term {
		rf.voted_for = -1
	}
	rf.current_term = new_term

	if new_leader != -1 {
		rf.receive_from_leader = true
	} else {
		rf.receive_from_leader = false
	}

	rf.leader_id = new_leader
}

//use this function when hold the lock
func (rf *Raft) becomeLeader() {
	rf.status = LEADER
	rf.next_index[rf.me] = len(rf.log)

	// NOTE: send heartbeat ASAP
	rf.sendOneRoundHeartBeat()
	go rf.sendHeartBeat()

	// FIXME: cause error when become leader twice
	go rf.leaderUpdateCommitIndex(rf.current_term)

	for i := 0; i < rf.all_server_number; i++ {
		// FIXME: cause error when become leader twice
		go rf.handleAppendEntryForOneServer(i, rf.current_term)
	}

	log.Printf("Server[%d] become LEADER at term %d", rf.me, rf.current_term)
	return
}

// The Ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		duration := rand.Intn(rf.timeout_rand_ms) + rf.timeout_const_ms
		// log.Printf("Server[%d] ticker: new wait time is %d(ms)", rf.me, duration)

		time.Sleep(time.Duration(duration) * time.Millisecond)
		log.Print("Server[", rf.me, "] goroutine number is ", runtime.NumGoroutine())

		rf.mu.Lock()

		// if I am leader
		if rf.status == LEADER {
			rf.mu.Unlock()
			continue
		}

		// if have a leader
		if rf.receive_from_leader {
			rf.receive_from_leader = false
			rf.mu.Unlock()
			continue
		}

		if rf.status == FOLLOWER {
			rf.becomePreCandidate()
			rf.mu.Unlock()
			continue
		}

		if rf.status == PRECANDIDATE {
			rf.becomeFollower(rf.current_term, rf.leader_id)
			rf.mu.Unlock()

			log.Printf("Server[%d] did not finish pre vote in time, become follower", rf.me)
			continue
		}

		if rf.status == CANDIDATE {
			// NOTE: only start pre vote once?
			rf.becomeCandidate(rf.current_term + 1)
			rf.mu.Unlock()

			log.Printf("Server[%d] did not finish vote in time, candidate term + 1, new term is %d", rf.me, rf.current_term)
			continue
		}

		// code unreachable
		log.Printf("Server[%d] reach code unreachable in ticker", rf.me)
		rf.mu.Unlock()

	}
}

func initLogSetting() {
	file, err := os.OpenFile("/tmp/tmp-fs/raft.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
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
	initLogSetting()

	rf.mu.Lock()

	rf.voted_for = -1
	rf.leader_id = -1
	rf.status = FOLLOWER
	rf.all_server_number = len(rf.peers)
	rf.quorum_number = (rf.all_server_number + 1) / 2
	rf.timeout_const_ms = 250
	rf.timeout_rand_ms = 150
	rf.match_index = make([]int, rf.all_server_number)
	rf.next_index = make([]int, rf.all_server_number)

	rf.recently_commit = make(chan ServerCommitIndex, rf.all_server_number)
	rf.append_entry_chan = make([]chan *RequestAppendEntryArgs, rf.all_server_number)
	rf.apply_ch = applyCh

	for i := 0; i < rf.all_server_number; i++ {
		rf.next_index[i] = 1
		rf.append_entry_chan[i] = make(chan *RequestAppendEntryArgs, rf.all_server_number)
	}

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
