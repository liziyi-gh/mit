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
	"bytes"

	"log"
	"math/rand"
	"os"

	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

func (self *Log) IsSameLog(index int, term int) bool {
	return self.INDEX == index && self.TERM == term
}

const (
	FOLLOWER = iota
	PRECANDIDATE
	CANDIDATE
	LEADER
)

const None = -1

func findLogMatchedIndex(logs []Log, term int, index int) (bool, int) {
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i].INDEX == index && logs[i].TERM == term {
			return true, logs[i].INDEX
		}
	}

	return false, None
}

// lambda expression is so unconvenience in Go
func findIndexOfFirstLogMatchedTerm(logs []Log, term int) (bool, int) {
	for i := 0; i <= len(logs)-1; i++ {
		if logs[i].TERM == term {
			return true, logs[i].INDEX
		}
	}

	return false, None
}

func findLastIndexOfTerm(logs []Log, term int) (bool, int) {
	for i := 0; i <= len(logs)-1; i++ {
		if logs[i].TERM <= term {
			return true, logs[i].INDEX
		}
	}

	return false, None
}

func findLastIndexbeforeTerm(logs []Log, term int) (bool, int) {
	for i := 0; i <= len(logs)-1; i++ {
		if logs[i].TERM < term {
			return true, logs[i].INDEX
		}
	}

	return false, None
}

func findTopK(target []int, k int) int {
	mut_target := make([]int, len(target))
	copy(mut_target, target)
	sort.Slice(mut_target, func(i, j int) bool {
		return mut_target[i] < mut_target[j]
	})

	return mut_target[len(mut_target)-k]
}

// A Go object implementing a single Raft peer.
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
	commit_index           int
	last_applied           int
	commit_index_in_quorom int

	// Volatile on leaders
	next_index  []int
	match_index []int
	status      int

	// Snapshot
	last_log_index_in_snapshot int
	last_log_term_in_snapshot  int
	snapshot_data              []byte

	// Convinence chanel
	recently_commit     chan struct{}
	append_entry_chan   []chan struct{}
	apply_ch            chan ApplyMsg
	internal_apply_chan chan ApplyMsg

	// Convinence constants
	all_server_number      int
	quorum_number          int
	timeout_const_ms       int
	timeout_rand_ms        int
	rpc_retry_times        int
	rpc_retry_interval_ms  int
	heartbeat_interval_ms  int
	chanel_buffer_size     int
	enable_feature_prevote bool
}

// use this function with lock
func (rf *Raft) GetPositionByIndex(index int) (int, bool) {
	for i := rf.LogLength() - 1; i >= 0; i-- {
		if rf.log[i].INDEX == index {
			return i, true
		}
	}
	return 0, false
}

// use this function with lock
func (rf *Raft) GetIndexByPosition(position int) int {
	return rf.log[position].INDEX
}

// use this function with lock
func (rf *Raft) LogLength() int {
	return len(rf.log)
}

// use this function with lock
func (rf *Raft) GetLogTermByIndex(index int) int {
	position, _ := rf.GetPositionByIndex(index)
	return rf.log[position].TERM
}

// use this function with lock
func (rf *Raft) GetLogCommandByIndex(index int) (interface{}, bool) {
	position, ok := rf.GetPositionByIndex(index)
	if !ok {
		log.Printf("Server[%d] get command by index [%d] failed", rf.me, index)
		return struct{}{}, false
	}
	return rf.log[position].COMMAND, true
}

// use this function with lock
func (rf *Raft) HaveAnyLog() bool {
	return rf.LogLength() >= 1
}

// use this function with lock
func (rf *Raft) GetLatestLogRef() *Log {
	if rf.HaveAnyLog() {
		return &rf.log[len(rf.log)-1]
	}
	return nil
}

// use this function with lock
func (rf *Raft) GetLatestLogIndex() int {
	if rf.HaveAnyLog() {
		return rf.log[len(rf.log)-1].INDEX
	}
	return 0
}

// use this function with lock
func (rf *Raft) GetLatestLogIndexIncludeSnapshot() int {
	if rf.HaveAnyLog() {
		return rf.log[len(rf.log)-1].INDEX
	}
	if rf.last_log_index_in_snapshot > 0 {
		return rf.last_log_index_in_snapshot
	}
	return 0
}

// use this function with lock
func (rf *Raft) GetLatestLogTermIncludeSnapshot() int {
	if rf.HaveAnyLog() {
		return rf.log[len(rf.log)-1].TERM
	}
	if rf.last_log_term_in_snapshot > 0 {
		return rf.last_log_term_in_snapshot
	}
	return 0
}

// use this function with lock
func (rf *Raft) AppendLog(new_log *Log) bool {
	rf.log = append(rf.log, *new_log)
	rf.persist()
	return true
}

// use this function with lock
func (rf *Raft) AppendLogs(logs []Log) bool {
	rf.log = append(rf.log, logs...)
	rf.persist()
	return true
}

// use this function with lock
// remove all logs that index > last_log_index
func (rf *Raft) RemoveLogIndexGreaterThan(last_log_index int) bool {
	reserve_logs_number := 0
	for i := 0; i < rf.LogLength(); i++ {
		if rf.log[i].INDEX == last_log_index {
			reserve_logs_number = i + 1
			break
		}
	}
	rf.log = rf.log[:reserve_logs_number]
	rf.persist()
	return true
}

// use this function with lock
// remove all logs that index < last_log_index
func (rf *Raft) RemoveLogIndexLessThan(last_log_index int) bool {
	reserve_logs_position := rf.LogLength()
	for i := 0; i < rf.LogLength(); i++ {
		if rf.log[i].INDEX == last_log_index {
			reserve_logs_position = i
			break
		}
	}
	rf.log = rf.log[reserve_logs_position:]
	rf.persist()
	return true
}

// use this function with lock
func (rf *Raft) SetTerm(new_term int) bool {
	rf.current_term = new_term
	rf.persist()
	return true
}

// use this function with lock
func (rf *Raft) SetVotefor(vote_for int) bool {
	rf.voted_for = vote_for
	rf.persist()
	return true
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term)
	e.Encode(rf.voted_for)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var current_term int
	var vote_for int
	logs := make([]Log, 0)
	if d.Decode(&current_term) != nil ||
		d.Decode(&vote_for) != nil ||
		d.Decode(&logs) != nil {
		log.Println("read persistent error")
	} else {
		rf.current_term = current_term
		rf.voted_for = vote_for
		rf.log = logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) doSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commit_index_in_quorom < index {
		log.Println("Server[", rf.me, "] give up Snapshot, some log not commit")
		return
	}
	rf.snapshot_data = snapshot
	rf.last_log_index_in_snapshot = index
	rf.last_log_term_in_snapshot = rf.GetLogTermByIndex(index)
	log.Println("Server[", rf.me, "] Snapshot index is ", index)
	log.Println("Server[", rf.me, "] have log before Snapshot", rf.log)
	rf.RemoveLogIndexLessThan(index + 1)
	log.Println("Server[", rf.me, "] have log after Snapshot", rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go rf.doSnapshot(index, snapshot)
}

func (rf *Raft) HasSnapshot() bool {
	return rf.last_log_index_in_snapshot > 0
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM           int
	CANDIDATE_ID   int
	PREV_LOG_INDEX int
	PREV_LOG_TERM  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
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
	UUID           uint64
}

type RequestInstallSnapshotArgs struct {
	TERM                int // leader's term
	LEADER_ID           int // for follower redirect clients
	LAST_INCLUDED_INDEX int
	LAST_INCLUDED_TERM  int
	OFFSET              int // not use
	DATA                []byte
	DONE                bool // not use
	UUID                uint64
}

type RequestInstallSnapshotReply struct {
	TERM int // current term, for leader to update itself
}

type RequestAppendEntryReply struct {
	TERM    int  // receiver's current term
	SUCCESS bool // true if contain matching prev log
	// the newest log index which has same term with RequestAppendEntryArgs.PREV_LOG_TERM
	// only valid when SUCCESS is false
	// None if no log in this term
	NEWST_LOG_INDEX_OF_PREV_LOG_TERM int
	LAST_LOG_INDEX_IN_SNAPSHOT       int
	LAST_LOG_TERM_IN_SNAPSHOT        int
}

func (rf *Raft) sendAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendOneAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
	ok := rf.sendAppendEntry(server, args, reply)
	failed_times := 0
	for !ok {
		if failed_times >= rf.rpc_retry_times {
			log.Print("Server[", rf.me, "] send append entry failed too many times ", failed_times, " to server ", server, "return")
			return false
		}

		rf.mu.Lock()
		if rf.current_term > args.TERM {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()

		ok = rf.sendAppendEntry(server, args, reply)
		rf.mu.Lock()
		if ok {
			if reply.TERM > rf.current_term {
				rf.becomeFollower(reply.TERM, None)
			}

			// maybe response lost, so need send signal
			if reply.SUCCESS {
				rf.successAppend(server, args.TERM, args)
			}
		}
		rf.mu.Unlock()
		failed_times++
		time.Sleep(time.Duration(rf.rpc_retry_interval_ms) * time.Millisecond)
	}

	return true
}

func (rf *Raft) leaderUpdateCommitIndex(current_term int) {
	for {
		if rf.killed() {
			return
		}

		<-rf.recently_commit

		rf.mu.Lock()
		if current_term != rf.current_term {
			rf.mu.Unlock()
			return
		}

		lowest_commit_index := findTopK(rf.next_index, rf.quorum_number) - 1
		if lowest_commit_index <= 0 || lowest_commit_index > rf.GetLatestLogIndexIncludeSnapshot() {
			rf.mu.Unlock()
			continue
		}

		// NOTE: prevent counting number to commit previous term's log
		lowest_commit_term := rf.GetLogTermByIndex(lowest_commit_index)
		if lowest_commit_term != current_term {
			rf.mu.Unlock()
			continue
		}

		rf.updateCommitIndex(lowest_commit_index)
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex(new_commit_index int) {
	if new_commit_index <= rf.commit_index {
		return
	}

	for idx := rf.commit_index + 1; idx <= new_commit_index && idx <= rf.GetLatestLogIndex(); idx++ {
		command, ok := rf.GetLogCommandByIndex(idx)
		if !ok {
			return
		}
		tmp := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: idx,
		}
		log.Printf("Server[%d] send index %d to internal channel", rf.me, tmp.CommandIndex)
		rf.internal_apply_chan <- tmp
		rf.commit_index_in_quorom = idx
	}

}

func (rf *Raft) sendCommandToApplierFunction() {
	for {
		new_command := <-rf.internal_apply_chan
		log.Printf("Server[%d] get new command index %d", rf.me, new_command.CommandIndex)
		rf.mu.Lock()
		if new_command.CommandIndex > rf.commit_index {
			log.Printf("Server[%d] going to commit index %d", rf.me, new_command.CommandIndex)
			rf.mu.Unlock()
			rf.apply_ch <- new_command
			log.Printf("Server[%d] commit index %d", rf.me, new_command.CommandIndex)
			rf.mu.Lock()
			rf.commit_index = new_command.CommandIndex
		}
		rf.mu.Unlock()
	}
}

// use this function with lock
// TODO: should refactor this, mix heartbeat and append log
func (rf *Raft) sendOneRoundHeartBeat() {
	i := 0
	args := make([]RequestAppendEntryArgs, rf.all_server_number)
	reply := make([]RequestAppendEntryReply, rf.all_server_number)

	for i = 0; i < rf.all_server_number; i++ {
		argi := &args[i]
		argi.TERM = rf.current_term
		argi.LEADER_ID = rf.me
		argi.LEADER_COMMIT = rf.commit_index
		argi.PREV_LOG_INDEX = rf.GetLatestLogIndexIncludeSnapshot()
		argi.PREV_LOG_TERM = rf.GetLatestLogTermIncludeSnapshot()
		// don't send heart beat to myself
		if i == args[i].LEADER_ID {
			continue
		}
		go rf.sendOneAppendEntry(i, argi, &reply[i])
	}
}

func (rf *Raft) sendHeartBeat(this_term int) {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(time.Duration(rf.heartbeat_interval_ms) * time.Millisecond)
		rf.mu.Lock()
		if !rf.statusIs(LEADER) {
			rf.mu.Unlock()
			return
		}

		if rf.current_term != this_term {
			rf.mu.Unlock()
			return
		}

		rf.sendOneRoundHeartBeat()
		rf.mu.Unlock()
	}
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Server[%d] install snapshot from [%d]", rf.me, args.LEADER_ID)

	reply.TERM = rf.current_term
	if rf.current_term > args.TERM {
		return
	}

	position, found_log_index := rf.GetPositionByIndex(args.LAST_INCLUDED_INDEX)
	if found_log_index {
		has_same_term := rf.log[position].TERM == args.LAST_INCLUDED_TERM
		if has_same_term {
			return
		}
	}

	// discard the entire log
	rf.RemoveLogIndexGreaterThan(0)

	// restore state machine using snapshot contents,
	// and load snapshot's cluster configuration
	apply_msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.DATA,
		SnapshotTerm:  args.LAST_INCLUDED_TERM,
		SnapshotIndex: args.LAST_INCLUDED_INDEX,
	}
	rf.apply_ch <- apply_msg
	rf.last_log_term_in_snapshot = args.LAST_INCLUDED_TERM
	rf.last_log_index_in_snapshot = args.LAST_INCLUDED_INDEX
	rf.last_applied = args.LAST_INCLUDED_INDEX
}

func (rf *Raft) buildReplyForAppendEntryFailed(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	found, index := findIndexOfFirstLogMatchedTerm(rf.log, args.PREV_LOG_TERM)
	if !found {
		reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM = None
		return
	}
	reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM = index
}

func (rf *Raft) tryAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	log.Print("Server[", rf.me, "] have log", rf.log)
	log.Print("Server[", rf.me, "] got append log args:", args)

	append_logs := args.ENTRIES

	if rf.HaveAnyLog() {
		matched, matched_log_index := findLogMatchedIndex(rf.log, args.PREV_LOG_TERM, args.PREV_LOG_INDEX)
		matched_log_position, _ := rf.GetPositionByIndex(matched_log_index)
		iter_self_log_position := matched_log_position + 1

		// prev log match
		if matched {
			for j := len(args.ENTRIES) - 1; j >= 0; j-- {
				if iter_self_log_position >= rf.LogLength() {
					goto there
				}
				iter_args_log := &args.ENTRIES[j]
				iter_self_log_idx := rf.GetIndexByPosition(iter_self_log_position)
				not_same_term := iter_args_log.TERM != rf.GetLogTermByIndex(iter_self_log_idx)
				not_same_index := iter_args_log.INDEX != iter_self_log_idx
				dismatch := not_same_term || not_same_index
				if dismatch {
					rf.RemoveLogIndexGreaterThan(rf.GetIndexByPosition(iter_self_log_position - 1))
					goto there
				}
				iter_self_log_position++
				append_logs = append_logs[:len(append_logs)-1]
			}
		}

		// prev log dismatched
		if !matched {
			if args.PREV_LOG_INDEX == 0 && args.PREV_LOG_TERM == 0 {
				rf.RemoveLogIndexGreaterThan(0)
			} else {
				log.Printf("Server[%d] Append Entry failed because PREV_LOG_INDEX %d not matched", rf.me, args.PREV_LOG_INDEX)
				rf.buildReplyForAppendEntryFailed(args, reply)
				return
			}
		}
	}
there:

	// if have no log
	if len(append_logs) > 0 && !rf.HaveAnyLog() {
		if args.PREV_LOG_INDEX != 0 || args.PREV_LOG_TERM != 0 {
			log.Print("Server[", rf.me, "] append log to empty failed")
			rf.buildReplyForAppendEntryFailed(args, reply)
			return
		}
	}

	// rpc call success
	reply.SUCCESS = true
	log.Println("Server[", rf.me, "]Success append log UUID: ", args.UUID)

	for i, j := 0, len(append_logs)-1; i < j; i, j = i+1, j-1 {
		append_logs[i], append_logs[j] = append_logs[j], append_logs[i]
	}

	rf.AppendLogs(append_logs)
	if len(append_logs) > 0 {
		log.Print("Server[", rf.me, "] new log is:", rf.log)
	}

	return
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.SUCCESS = false
	reply.TERM = rf.current_term
	reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM = None
	reply.LAST_LOG_TERM_IN_SNAPSHOT = rf.last_log_term_in_snapshot
	reply.LAST_LOG_INDEX_IN_SNAPSHOT = rf.last_log_index_in_snapshot
	if args.TERM < rf.current_term {
		log.Printf("Server[%d] reject Append Entry RPC from server[%d]", rf.me, args.LEADER_ID)
		return
	}

	if args.LEADER_ID != rf.me {
		rf.becomeFollower(args.TERM, args.LEADER_ID)
	}

	if len(args.ENTRIES) == 0 {
		matched, _ := findLogMatchedIndex(rf.log, args.PREV_LOG_TERM, args.PREV_LOG_INDEX)
		prev_log_same_as_snapshot := args.PREV_LOG_TERM == rf.last_log_term_in_snapshot &&
			args.PREV_LOG_INDEX == rf.last_log_index_in_snapshot
		matched = matched || prev_log_same_as_snapshot
		if !matched {
			return
		}
		// prevent heartbeat commit some logs that should not commit
		if rf.HaveAnyLog() {
			if matched && args.PREV_LOG_INDEX <= args.LEADER_COMMIT {
				log.Print("Server[", rf.me, "] going to commit args:", args)
				log.Print("Server[", rf.me, "] have log", rf.log)
				rf.updateCommitIndex(args.PREV_LOG_INDEX)
			}
		}
		reply.SUCCESS = true
		return
	}

	rf.tryAppendEntry(args, reply)

	return
}

func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.TERM = rf.current_term
	reply.VOTE_GRANTED = false
	log.Printf("Server[%d] got pre vote request from Server[%d]", rf.me, args.CANDIDATE_ID)

	// I have newer term
	if args.TERM < rf.current_term {
		log.Printf("Server[%d] reject pre vote request from Server[%d], term too low", rf.me, args.CANDIDATE_ID)
		return
	}

	// I have newer log
	if rf.HaveAnyLog() {
		my_latest_log := rf.GetLatestLogRef()

		if my_latest_log.TERM > args.PREV_LOG_TERM {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
			return
		}

		if (my_latest_log.TERM == args.PREV_LOG_TERM) && (my_latest_log.INDEX > args.PREV_LOG_INDEX) {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log, 2nd case", rf.me, args.CANDIDATE_ID)
			return
		}
	}

	if rf.HasSnapshot() && !rf.HaveAnyLog() {
		if rf.last_log_term_in_snapshot > args.PREV_LOG_TERM {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
			return
		}
		if (rf.last_log_term_in_snapshot == args.PREV_LOG_TERM) && (rf.last_log_index_in_snapshot > args.PREV_LOG_INDEX) {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log, 2nd case", rf.me, args.CANDIDATE_ID)
			return
		}
	}

	reply.VOTE_GRANTED = true
	if rf.current_term < args.TERM-1 {
		rf.becomeFollower(args.TERM-1, None)
	}

	log.Printf("Server[%d] granted pre vote request from Server[%d]", rf.me, args.CANDIDATE_ID)

	return
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Println("Server[", rf.me, "] got vote request args is ", args)

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
		rf.status = FOLLOWER
		rf.SetTerm(args.TERM)
		rf.SetVotefor(None)
	}

	// I have voted for other server
	if rf.voted_for != -1 && rf.voted_for != args.CANDIDATE_ID {
		log.Printf("Server[%d] reject vote request from %d, because have voted server[%d], at term %d",
			rf.me, args.CANDIDATE_ID, rf.voted_for, rf.current_term)
		return
	}

	// I have newer log
	if rf.HaveAnyLog() {
		my_latest_log := rf.GetLatestLogRef()

		if my_latest_log.TERM > args.PREV_LOG_TERM {
			log.Printf("Server[%d] reject vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
			return
		}

		if (my_latest_log.TERM == args.PREV_LOG_TERM) && (my_latest_log.INDEX > args.PREV_LOG_INDEX) {
			log.Printf("Server[%d] reject vote request from %d, because have newer log, 2nd case", rf.me, args.CANDIDATE_ID)
			return
		}
	}

	if rf.HasSnapshot() && !rf.HaveAnyLog() {
		if rf.last_log_term_in_snapshot > args.PREV_LOG_TERM {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
			return
		}
		if (rf.last_log_term_in_snapshot == args.PREV_LOG_TERM) && (rf.last_log_index_in_snapshot > args.PREV_LOG_INDEX) {
			log.Printf("Server[%d] reject pre vote request from %d, because have newer log, 2nd case", rf.me, args.CANDIDATE_ID)
			return
		}
	}

	// I can voted candidate now
	log.Printf("Server[%d] vote for Server[%d], at term %d", rf.me, args.CANDIDATE_ID, rf.current_term)
	reply.VOTE_GRANTED = true
	rf.SetTerm(args.TERM)
	rf.SetVotefor(args.CANDIDATE_ID)
}

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
	if rf.HaveAnyLog() || rf.HasSnapshot() {
		args.PREV_LOG_INDEX = rf.GetLatestLogIndexIncludeSnapshot()
		args.PREV_LOG_TERM = rf.GetLatestLogTermIncludeSnapshot()
	} else {
		args.PREV_LOG_TERM = -1
		args.PREV_LOG_INDEX = -1
	}
	rf.mu.Unlock()

	for failed_times := 0; failed_times < rf.rpc_retry_times; failed_times++ {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.current_term != this_round_term {
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

		ok := rf.sendRequestVote(index, args, reply)

		if !ok {
			time.Sleep(time.Duration(rf.rpc_retry_interval_ms) * time.Millisecond)
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
	return
}

func (rf *Raft) requestOneServerPreVote(index int, ans chan RequestVoteReply, this_round_term int) {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	rf.mu.Lock()
	args.TERM = this_round_term + 1
	args.CANDIDATE_ID = rf.me

	if rf.HaveAnyLog() || rf.HasSnapshot() {
		args.PREV_LOG_INDEX = rf.GetLatestLogIndexIncludeSnapshot()
		args.PREV_LOG_TERM = rf.GetLatestLogTermIncludeSnapshot()
	} else {
		args.PREV_LOG_TERM = -1
		args.PREV_LOG_INDEX = -1
	}
	rf.mu.Unlock()
	log.Printf("Server[%d] start requestOneServerPreVote", rf.me)

	failed_times := 0

	for {
		if rf.killed() {
			log.Printf("one gorountine DONE")
			return
		}
		ok := false

		if failed_times > rf.rpc_retry_times {
			log.Printf("Server[%d] requestOneServerPreVote failed too many times", rf.me)
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
			failed_times++
			time.Sleep(time.Duration(rf.rpc_retry_interval_ms) * time.Millisecond)
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
		if rf.killed() {
			return
		}

		select {
		case vote_reply := <-reply:
			log.Printf("Server[%d] got vote reply, granted is %t", rf.me, vote_reply.VOTE_GRANTED)
			rf.mu.Lock()
			if vote_reply.TERM > rf.current_term {
				rf.becomeCandidate(vote_reply.TERM)
				rf.mu.Unlock()
				return
			}

			// term is new term
			if this_round_term != rf.current_term {
				rf.mu.Unlock()
				log.Printf("Server[%d] quit last vote, because term %d is old", rf.me, this_round_term)
				return
			}

			// current status is not candidate anymore
			if rf.status != CANDIDATE {
				rf.mu.Unlock()
				return
			}

			if !vote_reply.VOTE_GRANTED {
				rf.mu.Unlock()
				continue
			}

			got_tickets++
			log.Printf("Server[%d] got vote tickets number is %d", rf.me, got_tickets)

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
		}
	}
}

func (rf *Raft) newPreVote(this_round_term int) bool {
	rf.mu.Lock()
	if this_round_term != rf.current_term {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	log.Printf("Server[%d] start new pre vote", rf.me)

	got_tickets := 0
	got_reply := 0
	reply := make(chan RequestVoteReply, rf.all_server_number)
	for i := 0; i < rf.all_server_number; i++ {
		go rf.requestOneServerPreVote(i, reply, this_round_term)
	}

	timeout_ms := rf.timeout_const_ms + rf.timeout_rand_ms

	for {
		if rf.killed() {
			log.Printf("newPreVote gorountine DONE")
			return false
		}

		select {
		case pre_vote_reply := <-reply:
			got_reply++
			log.Printf("Server[%d] got pre vote reply, granted is %t", rf.me, pre_vote_reply.VOTE_GRANTED)
			rf.mu.Lock()
			if pre_vote_reply.TERM > rf.current_term {
				rf.becomeFollower(pre_vote_reply.TERM, -1)
				rf.mu.Unlock()
				return false
			}

			// term is new term
			if rf.current_term != this_round_term {
				rf.status = FOLLOWER
				rf.mu.Unlock()
				log.Printf("Server[%d] quit pre vote, not term %d anymore", rf.me, this_round_term)
				return false
			}

			// current status is not pre-candidate anymore
			if rf.status != PRECANDIDATE {
				rf.mu.Unlock()
				log.Printf("Server[%d] quit pre vote, not PRECANDIDATE anymore", rf.me)
				return false
			}

			if !pre_vote_reply.VOTE_GRANTED {
				rf.mu.Unlock()
				continue
			}

			got_tickets += 1
			log.Printf("Server[%d] got pre vote tickets number is %d", rf.me, got_tickets)

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
		}
	}
}

// use this function when hold lock
func (rf *Raft) successAppend(server int, this_round_term int,
	args *RequestAppendEntryArgs) {
	log.Println("successAppend for ", server, "  handle args : ", args)

	if server != rf.me {
		newest_index_in_args := 0
		for _, value := range args.ENTRIES {
			if value.INDEX > newest_index_in_args {
				newest_index_in_args = value.INDEX
			}
		}
		new_next_index := newest_index_in_args + 1
		if new_next_index > rf.next_index[server] {
			log.Print("leader update next index for Server[", server, "] , new next index is ", new_next_index)
			rf.next_index[server] = new_next_index
		}
	}
	rf.recently_commit <- struct{}{}
}

// use this function when hold lock
func (rf *Raft) backwardArgsWhenAppendEntryFailed(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	initil_log_position := rf.LogLength() - 1
	new_prev_log_position := args.PREV_LOG_INDEX - 2
	new_entries := make([]Log, 0)

	// peer have at leaest 1 log in args.PREV_LOG_TERM
	if reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM != None {
		// move prev log to previous term's last log
		ok, last_index_of_prev_term := findLastIndexOfTerm(rf.log, args.PREV_LOG_TERM)
		if ok {
			new_prev_log_position = last_index_of_prev_term - 1
		}
	}

	// peer do not have log in args.PREV_LOG_TERM
	if reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM == None {
		ok, last_index_before_term := findLastIndexbeforeTerm(rf.log, args.PREV_LOG_TERM)
		if ok {
			new_prev_log_position = last_index_before_term - 1
		} else {
			if new_prev_log_position >= 0 {
				new_prev_log_position = 0
			}
		}
	}

	// add logs from latest to prev_log_position to args
	for i := initil_log_position; i > new_prev_log_position; i-- {
		new_entries = append(new_entries, rf.log[i])
	}
	args.ENTRIES = new_entries

	if new_prev_log_position >= 0 {
		new_prev_log_idx := new_prev_log_position + 1
		args.PREV_LOG_TERM = rf.GetLogTermByIndex(new_prev_log_idx)
		args.PREV_LOG_INDEX = new_prev_log_idx
	} else {
		args.PREV_LOG_TERM = 0
		args.PREV_LOG_INDEX = 0
	}
}

// use with the lock
func (rf *Raft) buildNewestArgs() *RequestAppendEntryArgs {
	// TODO: reduce rpc number, from latest_log to next_index
	latest_log := *rf.GetLatestLogRef()
	append_logs := []Log{latest_log}
	prev_log_index := latest_log.INDEX - 1
	prev_log_term := 0
	if prev_log_index > 0 {
		prev_log_term = rf.GetLogTermByIndex(prev_log_index)
	}
	args := &RequestAppendEntryArgs{
		TERM:           rf.current_term,
		LEADER_ID:      rf.me,
		ENTRIES:        append_logs,
		LEADER_COMMIT:  rf.commit_index,
		PREV_LOG_INDEX: prev_log_index,
		PREV_LOG_TERM:  prev_log_term,
		UUID:           rand.Uint64(),
	}

	return args
}

func (rf *Raft) sendSnapshotOnetime(server int, args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, this_round_term int) {
	// // FIXME: implement this function
	// args := &RequestInstallSnapshotArgs{
	// 	TERM:                rf.current_term,
	// 	LEADER_ID:           rf.me,
	// 	LAST_INCLUDED_INDEX: rf.last_log_index_in_snapshot,
	// 	LAST_INCLUDED_TERM:  rf.last_log_term_in_snapshot,
	// 	DATA:                rf.snapshot_data,
	// }
	// reply := &RequestInstallSnapshotReply{}
	// rf.sendSnapshotOnetime(server, args, reply)
	log.Println("LEADER should send snapshot to Server", server)
}

func (rf *Raft) sendNewestLog(server int, this_round_term int, ch chan struct{}) {
	<-ch
	defer func() { ch <- struct{}{} }()
	rf.mu.Lock()

	if rf.current_term != this_round_term {
		rf.mu.Unlock()
		return
	}

	// NOTE: if use as heartbeat, delete this
	if !rf.HaveAnyLog() {
		rf.mu.Unlock()
		return
	}

	args := rf.buildNewestArgs()
	log.Print("Server[", rf.me, "] running handleAppendEntryForOneServer for server", server, "args is ", args)
	failed_times := 0

	// reduce rpc number
	if len(args.ENTRIES) > 0 && args.ENTRIES[0].INDEX < rf.next_index[server] && rf.me != server {
		// NOTE: why here cause problem? should promise next_index would not update by mistake,
		// so should promise update next_index in right term
		log.Print("Server[", rf.me, "] skip args ", args, "because peer ", server, " already have")
		goto end
	}

	if server == rf.me {
		rf.successAppend(server, this_round_term, args)
		goto end
	}
	rf.mu.Unlock()

	// TODO: refactor, extract as a function
	for {
		reply := &RequestAppendEntryReply{}
		ok := rf.sendOneAppendEntry(server, args, reply)
		rf.mu.Lock()
		if !ok {
			goto end
		}

		// new term case 1, update term by other way
		if this_round_term != rf.current_term {
			goto end
		}

		if reply.SUCCESS {
			// TODO: why can not delete this success append?
			// something about the lock? it's not happen every time.
			rf.successAppend(server, this_round_term, args)
			goto end
		}

		// new term case 2, know from peer
		if reply.TERM > rf.current_term {
			rf.becomeFollower(reply.TERM, -1)
			goto end
		}

		// reply is false
		failed_times++
		log.Print("Server[", server, "] failed time: ", failed_times, ", args is ", args)

		need_snapshot := rf.next_index[server] <= rf.last_log_index_in_snapshot

		if need_snapshot {
			rf.mu.Unlock()
			go rf.sendSnapshot(server, this_round_term)
			return
		} else {
			// log not match
			if args.PREV_LOG_INDEX == 0 && args.PREV_LOG_TERM == 0 {
				goto end
			}
			rf.backwardArgsWhenAppendEntryFailed(args, reply)
			log.Print("Server[", server, "] log dismatch, new args is ", args)
			rf.mu.Unlock()
			continue
		}
		// lock has been release here, no code below
	} // end reply false for

end:
	rf.mu.Unlock()
	return
}

func (rf *Raft) handleAppendEntryForOneServer(server int, this_round_term int) {
	log.Printf("Server[%d] enter handleAppendEntryForOneServer", rf.me)
	defer log.Print("Server[", server, "] quit handleAppendEntryForOneServer")
	worker_number := 3
	ch := make(chan struct{}, worker_number)
	for i := 0; i < worker_number; i++ {
		ch <- struct{}{}
	}

	for {
		if rf.killed() {
			log.Printf("one gorountine DONE")
			return
		}

		rf.mu.Lock()
		if rf.current_term != this_round_term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		select {
		// TODO: here block, so should use go if want multipy worker, but prevent too much RPC.
		// the append_entry_chan is just one time invoke, so need timer to
		// append log continuously, otherwise it's too slow
		case <-time.After(time.Duration(rf.heartbeat_interval_ms) * time.Millisecond):
			rf.sendNewestLog(server, this_round_term, ch)
		case <-rf.append_entry_chan[server]:
			rf.sendNewestLog(server, this_round_term, ch)
		}
	}

}

func (rf *Raft) newRoundAppend(command interface{}, index int) {
	for i := 0; i < rf.all_server_number; i++ {
		rf.append_entry_chan[i] <- struct{}{}
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	new_log_index := None
	term := None
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// is not leader
	if !rf.statusIs(LEADER) {
		return new_log_index, term, false
	}
	term = rf.current_term
	isLeader = true
	new_log_index = rf.GetLatestLogIndexIncludeSnapshot() + 1
	rf.next_index[rf.me] = new_log_index + 1
	new_log := &Log{
		INDEX:   new_log_index,
		TERM:    rf.current_term,
		COMMAND: command,
	}
	rf.AppendLog(new_log)
	go rf.newRoundAppend(command, new_log_index)

	log.Print("Server[", rf.me, "] Start accept new log:", new_log)

	return new_log_index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	log.Printf("Kill one Raft server")
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
	rf.leader_id = None
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
	// NOTE: always update term then update voted for
	rf.SetTerm(new_term)
	rf.SetVotefor(None)
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

	prev_term := rf.current_term

	rf.SetTerm(new_term)

	if prev_term < new_term {
		rf.SetVotefor(None)
	}

	if new_leader != -1 {
		rf.receive_from_leader = true
	} else {
		rf.receive_from_leader = false
	}

	rf.leader_id = new_leader
}

// use this function when hold the lock
func (rf *Raft) becomeLeader() {
	rf.status = LEADER
	rf.next_index[rf.me] = rf.GetLatestLogIndexIncludeSnapshot() + 1

	// NOTE: send heartbeat ASAP
	rf.sendOneRoundHeartBeat()
	go rf.sendHeartBeat(rf.current_term)

	go rf.leaderUpdateCommitIndex(rf.current_term)

	for i := 0; i < rf.all_server_number; i++ {
		if i != rf.me {
			rf.next_index[i] = 1
		}
	}

	for i := 0; i < rf.all_server_number; i++ {
		go rf.handleAppendEntryForOneServer(i, rf.current_term)
	}
	log.Printf("Server[%d] become LEADER at term %d", rf.me, rf.current_term)
	log.Println("leader log is ", rf.log)
	return
}

// The Ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		if rf.killed() {
			return
		}

		duration := rand.Intn(rf.timeout_rand_ms) + rf.timeout_const_ms
		log.Printf("Server[%d] ticker: new wait time is %d(ms)", rf.me, duration)

		time.Sleep(time.Duration(duration) * time.Millisecond)

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

		switch rf.status {
		case FOLLOWER:
			if rf.enable_feature_prevote {
				rf.becomePreCandidate()
			} else {
				rf.becomeCandidate(rf.current_term + 1)
			}
		case PRECANDIDATE:
			rf.becomeFollower(rf.current_term, rf.leader_id)
			log.Printf("Server[%d] did not finish pre vote in time, become follower", rf.me)

		case CANDIDATE:
			rf.becomeCandidate(rf.current_term + 1)
			log.Printf("Server[%d] did not finish vote in time, candidate term + 1, new term is %d", rf.me, rf.current_term)
		}

		rf.mu.Unlock()
	}
}

func initLogSetting(me int) {
	file, err := os.OpenFile("/tmp/tmp-fs/raft.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	initLogSetting(me)

	rf.mu.Lock()

	rf.enable_feature_prevote = true
	rf.voted_for = None
	rf.leader_id = None
	rf.status = FOLLOWER
	rf.all_server_number = len(rf.peers)
	rf.quorum_number = (rf.all_server_number + 1) / 2
	rf.timeout_const_ms = 250
	rf.timeout_rand_ms = 150
	rf.match_index = make([]int, rf.all_server_number)
	rf.next_index = make([]int, rf.all_server_number)
	// NOTE: this is an arbitray number
	rf.chanel_buffer_size = 1000

	rf.recently_commit = make(chan struct{}, rf.chanel_buffer_size)
	rf.internal_apply_chan = make(chan ApplyMsg, rf.chanel_buffer_size)
	rf.append_entry_chan = make([]chan struct{}, rf.all_server_number)
	rf.apply_ch = applyCh
	rf.rpc_retry_times = 1
	rf.rpc_retry_interval_ms = 10
	rf.heartbeat_interval_ms = 100

	for i := 0; i < rf.all_server_number; i++ {
		rf.next_index[i] = 1
		// NOTE: this is an arbitray number
		rf.append_entry_chan[i] = make(chan struct{}, rf.chanel_buffer_size)
	}

	go rf.sendCommandToApplierFunction()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
