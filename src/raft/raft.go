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

const Raft_debug = true
const LOGPATH = "/tmp/tmp-fs/raft.log"
const (
	FOLLOWER = iota
	PRECANDIDATE
	CANDIDATE
	LEADER
)
const None = -1

func dPrintf(format string, a ...interface{}) {
	if Raft_debug {
		log.Printf(format, a...)
	}
	return
}

func dPrintln(a ...interface{}) {
	if Raft_debug {
		log.Println(a...)
	}
}

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
	quorom_commit_index int

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

func (rf *Raft) getPositionByIndex(index int) (int, bool) {
	for i := rf.logLength() - 1; i >= 0; i-- {
		if rf.log[i].INDEX == index {
			return i, true
		}
	}
	return 0, false
}

func (rf *Raft) getIndexByPosition(position int) int {
	return rf.log[position].INDEX
}

func (rf *Raft) logLength() int {
	return len(rf.log)
}

func (rf *Raft) getLogTermByIndex(index int) int {
	position, ok := rf.getPositionByIndex(index)
	if !ok {
		return 0
	}
	return rf.log[position].TERM
}

func (rf *Raft) getLogCommandByIndex(index int) (interface{}, bool) {
	position, ok := rf.getPositionByIndex(index)
	if !ok {
		dPrintf("Server[%d] get command by index [%d] failed", rf.me, index)
		return struct{}{}, false
	}
	return rf.log[position].COMMAND, true
}

func (rf *Raft) hasLog() bool {
	return rf.logLength() >= 1
}

func (rf *Raft) getLatestLogIndex() int {
	if rf.hasLog() {
		return rf.log[len(rf.log)-1].INDEX
	}
	return 0
}

func (rf *Raft) getLatestLogIndexIncludeSnapshot() int {
	if rf.hasLog() {
		return rf.log[len(rf.log)-1].INDEX
	}
	if rf.last_log_index_in_snapshot > 0 {
		return rf.last_log_index_in_snapshot
	}
	return 0
}

func (rf *Raft) getLatestLogTermIncludeSnapshot() int {
	if rf.hasLog() {
		return rf.log[len(rf.log)-1].TERM
	}
	if rf.last_log_term_in_snapshot > 0 {
		return rf.last_log_term_in_snapshot
	}
	return 0
}

func (rf *Raft) appendLogs(logs []Log) {
	rf.log = append(rf.log, logs...)
	rf.persist()
}

// remove all logs that index > last_log_index
func (rf *Raft) removeLogIndexGreaterThan(last_log_index int) bool {
	reserve_logs_number := 0
	for i := 0; i < rf.logLength(); i++ {
		if rf.log[i].INDEX == last_log_index {
			reserve_logs_number = i + 1
			break
		}
	}
	rf.log = rf.log[:reserve_logs_number]

	if last_log_index < rf.last_log_index_in_snapshot {
		rf.last_log_index_in_snapshot = 0
		rf.last_log_term_in_snapshot = 0
		rf.snapshot_data = nil
	}
	rf.persist()
	return true
}

// remove all logs that index < last_log_index
// is caller's job to call rf.persist
func (rf *Raft) removeLogIndexLessThan(last_log_index int) bool {
	reserve_logs_position := rf.logLength()
	for i := 0; i < rf.logLength(); i++ {
		if rf.log[i].INDEX == last_log_index {
			reserve_logs_position = i
			break
		}
	}
	rf.log = rf.log[reserve_logs_position:]
	return true
}

func (rf *Raft) setTerm(new_term int) {
	if rf.current_term != new_term {
		rf.current_term = new_term
		rf.persist()
	}
}

func (rf *Raft) setVotefor(vote_for int) {
	if vote_for != rf.voted_for {
		rf.voted_for = vote_for
		rf.persist()
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term)
	e.Encode(rf.voted_for)
	e.Encode(rf.last_log_term_in_snapshot)
	e.Encode(rf.last_log_index_in_snapshot)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot_data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var current_term int
	var vote_for int
	var last_log_term_in_snapshot int
	var last_log_index_in_snapshot int
	logs := make([]Log, 0)
	// TODO: should add check code for production enviroment

	ok := d.Decode(&current_term) == nil &&
		d.Decode(&vote_for) == nil &&
		d.Decode(&last_log_term_in_snapshot) == nil &&
		d.Decode(&last_log_index_in_snapshot) == nil &&
		d.Decode(&logs) == nil

	if !ok {
		dPrintln("Error: decode persist failed")
		return
	}

	rf.current_term = current_term
	rf.voted_for = vote_for
	rf.log = logs

	snapshot_data := rf.persister.ReadSnapshot()
	if snapshot_data != nil &&
		last_log_index_in_snapshot > 0 &&
		last_log_term_in_snapshot > 0 {
		rf.last_log_term_in_snapshot = last_log_term_in_snapshot
		rf.last_log_index_in_snapshot = last_log_index_in_snapshot
		rf.snapshot_data = snapshot_data
		command := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot_data,
			SnapshotTerm:  last_log_term_in_snapshot,
			SnapshotIndex: last_log_index_in_snapshot,
		}
		rf.quorom_commit_index = last_log_index_in_snapshot
		rf.internal_apply_chan <- command
	}
	dPrintln("Server", rf.me, "restore with",
		current_term, vote_for, last_log_term_in_snapshot, last_log_index_in_snapshot, logs)
	return
}

func (rf *Raft) doCondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// cocurrent trying to hold lock, if 2 requestInstallSnasphot send nearly same time,
	// one send to channel, and not enter this function yet,
	// another requestInstallSnasphot hold the lock and trying to send to channel
	// then requestInstallSnasphot hold the lock because consumer did not consume
	// so it would cause dead lock
	// what this function use for? I don't get it
	go rf.doCondInstallSnapshot(lastIncludedTerm, lastIncludedIndex, snapshot)

	return true
}

func (rf *Raft) doSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	err := !rf.hasLog() ||
		index < rf.log[0].INDEX ||
		index <= rf.last_log_index_in_snapshot
	if err {
		return
	}

	rf.snapshot_data = snapshot
	rf.last_log_index_in_snapshot = index
	rf.last_log_term_in_snapshot = rf.getLogTermByIndex(index)
	rf.removeLogIndexLessThan(index + 1)
	dPrintf("Server[%v] make snapshot, last idx is %v", rf.me, rf.last_log_index_in_snapshot)
	rf.persist()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.doSnapshot(index, snapshot)
}

func (rf *Raft) hasSnapshot() bool {
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
}

type RequestInstallSnapshotArgs struct {
	TERM                int // leader's term
	LEADER_ID           int // for follower redirect clients
	LAST_INCLUDED_INDEX int
	LAST_INCLUDED_TERM  int
	OFFSET              int // not use
	DATA                []byte
	DONE                bool // not use
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
	for failed_times := 0; failed_times < rf.rpc_retry_times; failed_times++ {
		if rf.sendAppendEntry(server, args, reply) {
			return true
		}

		time.Sleep(time.Duration(rf.rpc_retry_interval_ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.current_term > args.TERM {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()
	}

	return false
}

func (rf *Raft) leaderUpdateCommitIndex(current_term int) {
	for !rf.killed() {
		<-rf.recently_commit

		rf.mu.Lock()
		if current_term != rf.current_term {
			rf.mu.Unlock()
			return
		}

		// NOTE: prevent counting number to commit previous term's log
		lowest_commit_index := findTopK(rf.next_index, rf.quorum_number) - 1
		lowest_commit_term := rf.getLogTermByIndex(lowest_commit_index)
		if lowest_commit_index <= 0 ||
			lowest_commit_index > rf.getLatestLogIndexIncludeSnapshot() ||
			lowest_commit_term != current_term {
			rf.mu.Unlock()
			continue
		}

		rf.updateCommitIndex(lowest_commit_index)
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex(new_commit_index int) {
	if new_commit_index <= rf.quorom_commit_index ||
		new_commit_index <= rf.last_log_index_in_snapshot {
		return
	}

	for idx := rf.quorom_commit_index + 1; idx <= new_commit_index && idx <= rf.getLatestLogIndex(); idx++ {
		command, ok := rf.getLogCommandByIndex(idx)
		if !ok {
			return
		}
		tmp := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: idx,
		}
		dPrintf("Server[%d] send index %d to internal channel", rf.me, tmp.CommandIndex)
		// FIXME: use channel communicate can been block and cause dead lock
		rf.quorom_commit_index = idx
		rf.internal_apply_chan <- tmp
	}

}

func (rf *Raft) sendCommandToApplierFunction() {
	for !rf.killed() {
		new_command := <-rf.internal_apply_chan
		new_command_idx := 0
		if new_command.CommandValid {
			new_command_idx = new_command.CommandIndex
		}
		if new_command.SnapshotValid {
			new_command_idx = new_command.SnapshotIndex
		}
		dPrintf("Server[%d] get new command index %d", rf.me, new_command_idx)
		rf.apply_ch <- new_command
	}
}

// TODO: should refactor this, mix heartbeat and append log
func (rf *Raft) sendOneRoundHeartBeat() {
	args := make([]RequestAppendEntryArgs, rf.all_server_number)
	reply := make([]RequestAppendEntryReply, rf.all_server_number)

	for i := 0; i < rf.all_server_number; i++ {
		// don't send heart beat to myself
		if i == rf.me {
			continue
		}
		argi := &args[i]
		argi.TERM = rf.current_term
		argi.LEADER_ID = rf.me
		argi.LEADER_COMMIT = rf.quorom_commit_index
		argi.PREV_LOG_INDEX = rf.getLatestLogIndexIncludeSnapshot()
		argi.PREV_LOG_TERM = rf.getLatestLogTermIncludeSnapshot()
		go rf.sendOneAppendEntry(i, argi, &reply[i])
	}
}

func (rf *Raft) handleOneServerHeartbeat(server int, this_round_term int) {
	// FIXME: this is an arbitray number
	worker_number := 100
	ch := make(chan struct{}, worker_number)
	for i := 0; i < worker_number; i++ {
		ch <- struct{}{}
	}
	for !rf.killed() {
		time.Sleep(time.Duration(rf.heartbeat_interval_ms) * time.Millisecond)
		<-ch

		rf.mu.Lock()
		if rf.current_term != this_round_term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func() {
			rf.mu.Lock()
			args := &RequestAppendEntryArgs{}
			reply := &RequestAppendEntryReply{}
			args.TERM = rf.current_term
			args.LEADER_ID = rf.me
			args.LEADER_COMMIT = rf.quorom_commit_index
			args.PREV_LOG_INDEX = rf.getLatestLogIndexIncludeSnapshot()
			args.PREV_LOG_TERM = rf.getLatestLogTermIncludeSnapshot()
			rf.mu.Unlock()
			rf.sendOneAppendEntry(server, args, reply)
			ch <- struct{}{}
		}()
	}
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dPrintf("Server[%d] got install snapshot request from [%d], last log index is %d", rf.me, args.LEADER_ID, args.LAST_INCLUDED_INDEX)

	reply.TERM = rf.current_term
	if rf.current_term > args.TERM {
		dPrintf("Server[%d] Reject install snapshot from old leader", rf.me)
		return
	}
	if args.LAST_INCLUDED_INDEX == rf.last_log_index_in_snapshot &&
		args.LAST_INCLUDED_TERM == rf.last_log_term_in_snapshot {
		return
	}
	if args.LAST_INCLUDED_INDEX < rf.quorom_commit_index {
		return
	}

	dPrintf("Server[%d] install snapshot from [%d]", rf.me, args.LEADER_ID)
	rf.removeLogIndexGreaterThan(0)
	rf.last_log_term_in_snapshot = args.LAST_INCLUDED_TERM
	rf.last_log_index_in_snapshot = args.LAST_INCLUDED_INDEX
	rf.snapshot_data = args.DATA
	rf.persist()

	// restore state machine using snapshot contents,
	apply_msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.DATA,
		SnapshotTerm:  args.LAST_INCLUDED_TERM,
		SnapshotIndex: args.LAST_INCLUDED_INDEX,
	}
	rf.quorom_commit_index = args.LAST_INCLUDED_INDEX
	rf.internal_apply_chan <- apply_msg
}

func (rf *Raft) sendSnapshotOnetime(server int, args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, this_round_term int) {
	rf.mu.Lock()
	args := &RequestInstallSnapshotArgs{
		TERM:                rf.current_term,
		LEADER_ID:           rf.me,
		LAST_INCLUDED_INDEX: rf.last_log_index_in_snapshot,
		LAST_INCLUDED_TERM:  rf.last_log_term_in_snapshot,
		DATA:                rf.snapshot_data,
	}
	rf.mu.Unlock()
	reply := &RequestInstallSnapshotReply{}

	for failed_times := 0; failed_times < rf.rpc_retry_times; failed_times++ {
		ok := rf.sendSnapshotOnetime(server, args, reply)
		rf.mu.Lock()
		if rf.current_term != this_round_term {
			goto release_lock_and_return
		}
		if reply.TERM > this_round_term {
			goto release_lock_and_return
		}
		if ok {
			rf.next_index[server] = args.LAST_INCLUDED_INDEX + 1
			dPrintln("leader", rf.me, "send snapshot to Server", server)
			goto release_lock_and_return
		}
		rf.mu.Unlock()
	}

	dPrintln("leader", rf.me, "failed send snapshot to Server", server)
	return

release_lock_and_return:
	rf.mu.Unlock()
	return
}

func (rf *Raft) buildReplyForAppendEntryFailed(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	found, index := findIndexOfFirstLogMatchedTerm(rf.log, args.PREV_LOG_TERM)
	if !found {
		reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM = None
		return
	}
	reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM = index
}

func (rf *Raft) hasSnapshotLog(logs []Log) (bool, int) {
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i].INDEX == rf.last_log_index_in_snapshot &&
			logs[i].TERM == rf.last_log_term_in_snapshot {
			return true, i
		}
	}

	return false, -1
}

func (rf *Raft) sliceLogToAlign(args *RequestAppendEntryArgs) ([]Log, bool) {
	append_logs := args.ENTRIES
	matched, matched_log_index := findLogMatchedIndex(rf.log, args.PREV_LOG_TERM, args.PREV_LOG_INDEX)
	matched_log_position, _ := rf.getPositionByIndex(matched_log_index)
	iter_self_log_position := matched_log_position + 1

	if (args.PREV_LOG_INDEX == 0 && args.PREV_LOG_TERM == 0) ||
		(args.PREV_LOG_INDEX == rf.last_log_index_in_snapshot &&
			args.PREV_LOG_TERM == rf.last_log_term_in_snapshot) {
		matched = true
		iter_self_log_position = 0
	}

	// prev log match
	if matched {
		for j := len(args.ENTRIES) - 1; j >= 0; j-- {
			if iter_self_log_position >= rf.logLength() {
				return append_logs, true
			}
			iter_args_log := &args.ENTRIES[j]
			iter_self_log_idx := rf.getIndexByPosition(iter_self_log_position)
			is_same_term := iter_args_log.TERM == rf.getLogTermByIndex(iter_self_log_idx)
			is_same_index := iter_args_log.INDEX == iter_self_log_idx
			dismatch := !(is_same_term && is_same_index)
			if dismatch {
				rf.removeLogIndexGreaterThan(iter_self_log_idx - 1)
				return append_logs, true
			}
			iter_self_log_position++
			append_logs = append_logs[:len(append_logs)-1]
		}
		// if have all log, return true
		return []Log{}, true
	}

	// prev log dismatched
	if !matched {
		has_snapshot_log, snapshot_position := rf.hasSnapshotLog(append_logs)
		if has_snapshot_log {
			rf.removeLogIndexGreaterThan(rf.last_log_index_in_snapshot)
			append_logs = append_logs[:snapshot_position]
			return append_logs, true
		}

		if args.PREV_LOG_INDEX == rf.last_log_index_in_snapshot && args.PREV_LOG_TERM == rf.last_log_term_in_snapshot {
			rf.removeLogIndexGreaterThan(rf.last_log_index_in_snapshot)
			return append_logs, true
		}

		dPrintf("Server[%d] Append Entry failed because PREV_LOG_INDEX %d not matched", rf.me, args.PREV_LOG_INDEX)
		return []Log{}, false
	}
	return []Log{}, false
}

func (rf *Raft) tryAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	dPrintf("Server[%v] have log %v", rf.me, rf.log)
	dPrintf("Server[%v] got append log args %v:", rf.me, args)

	append_logs := args.ENTRIES
	ok := false

	if rf.hasLog() {
		append_logs, ok = rf.sliceLogToAlign(args)
		if !ok {
			rf.buildReplyForAppendEntryFailed(args, reply)
			return
		}
		goto start_append_logs
	}

	// if have no log
	if len(append_logs) > 0 && !rf.hasLog() {
		has_snapshot_log, snapshot_position := rf.hasSnapshotLog(append_logs)
		if has_snapshot_log {
			rf.removeLogIndexGreaterThan(rf.last_log_index_in_snapshot)
			append_logs = append_logs[:snapshot_position]
			goto start_append_logs
		}
		prev_log_is_snapshot_log := args.PREV_LOG_INDEX == rf.last_log_index_in_snapshot && args.PREV_LOG_TERM == rf.last_log_term_in_snapshot
		is_first_log := args.PREV_LOG_INDEX == 0 && args.PREV_LOG_TERM == 0
		if !is_first_log && !prev_log_is_snapshot_log {
			dPrintf("Server[%v] append empty failed", rf.me)
			rf.buildReplyForAppendEntryFailed(args, reply)
			return
		}
	}

start_append_logs:
	// rpc call success
	reply.SUCCESS = true

	// revert logs to normal order
	for i, j := 0, len(append_logs)-1; i < j; i, j = i+1, j-1 {
		append_logs[i], append_logs[j] = append_logs[j], append_logs[i]
	}

	rf.appendLogs(append_logs)
	if len(append_logs) > 0 {
		dPrintf("Server[%v] new log is %v", rf.me, rf.log)
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
		dPrintf("Server[%d] reject Append Entry RPC from server[%d]", rf.me, args.LEADER_ID)
		return
	}

	rf.becomeFollower(args.TERM, args.LEADER_ID)

	if len(args.ENTRIES) == 0 {
		matched, _ := findLogMatchedIndex(rf.log, args.PREV_LOG_TERM, args.PREV_LOG_INDEX)
		prev_log_same_as_snapshot := args.PREV_LOG_TERM == rf.last_log_term_in_snapshot &&
			args.PREV_LOG_INDEX == rf.last_log_index_in_snapshot
		matched = matched || prev_log_same_as_snapshot
		// prevent heartbeat commit some logs that should not commit
		if !matched {
			return
		}
		if rf.hasLog() && args.PREV_LOG_INDEX <= args.LEADER_COMMIT {
			rf.updateCommitIndex(args.PREV_LOG_INDEX)
		}
		reply.SUCCESS = true
		return
	}

	rf.tryAppendEntry(args, reply)

	return
}

func (rf *Raft) hasNewerLog(args *RequestVoteArgs) bool {
	if rf.hasLog() {
		my_latest_log := &rf.log[len(rf.log)-1]
		newer_case1 := my_latest_log.TERM > args.PREV_LOG_TERM
		newer_case2 := (my_latest_log.TERM == args.PREV_LOG_TERM) && (my_latest_log.INDEX > args.PREV_LOG_INDEX)
		return newer_case1 || newer_case2
	}

	if rf.hasSnapshot() {
		newer_case1 := rf.last_log_term_in_snapshot > args.PREV_LOG_TERM
		newer_case2 := (rf.last_log_term_in_snapshot == args.PREV_LOG_TERM) && (rf.last_log_index_in_snapshot > args.PREV_LOG_INDEX)
		return newer_case1 || newer_case2
	}

	return false
}

func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.TERM = rf.current_term
	reply.VOTE_GRANTED = false
	dPrintf("Server[%d] got prevote request from Server[%d]", rf.me, args.CANDIDATE_ID)

	// I have newer term
	if args.TERM < rf.current_term {
		dPrintf("Server[%d] reject prevote request from Server[%d], term too low", rf.me, args.CANDIDATE_ID)
		return
	}

	// I have newer log
	if rf.hasNewerLog(args) {
		dPrintf("Server[%d] reject prevote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
		return
	}

	reply.VOTE_GRANTED = true
	if rf.current_term < args.TERM-1 {
		rf.becomeFollower(args.TERM-1, None)
	}

	dPrintf("Server[%d] granted prevote request from Server[%d]", rf.me, args.CANDIDATE_ID)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dPrintf("Server[%v] got vote request args is %v", rf.me, args)

	reply.VOTE_GRANTED = false
	reply.TERM = rf.current_term

	// I have newer term
	if rf.current_term > args.TERM {
		dPrintf("Server[%d] reject vote request from %d, because its term %d lower than current term %d",
			rf.me, args.CANDIDATE_ID, args.TERM, rf.current_term)
		return
	}

	// caller term greater than us
	if rf.current_term < args.TERM {
		// NOTE: it would not cause mutilply leaders?
		// update: of course not! 1 term, 1 server, 1 ticket, fair enough.
		rf.becomeFollower(args.TERM, None)
	}

	// I have voted for other server
	if rf.voted_for != None && rf.voted_for != args.CANDIDATE_ID {
		dPrintf("Server[%d] reject vote request from %d, because have voted server[%d], at term %d",
			rf.me, args.CANDIDATE_ID, rf.voted_for, rf.current_term)
		return
	}

	// I have newer log
	if rf.hasNewerLog(args) {
		dPrintf("Server[%d] reject vote request from %d, because have newer log", rf.me, args.CANDIDATE_ID)
		return
	}

	// I can voted candidate now
	dPrintf("Server[%d] vote for Server[%d], at term %d", rf.me, args.CANDIDATE_ID, rf.current_term)
	reply.VOTE_GRANTED = true
	rf.setTerm(args.TERM)
	rf.setVotefor(args.CANDIDATE_ID)
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

func (rf *Raft) requestOneServerVote(index int, ans chan *RequestVoteReply, this_round_term int) {
	args := &RequestVoteArgs{}

	rf.mu.Lock()
	args.TERM = this_round_term
	args.CANDIDATE_ID = rf.me
	args.PREV_LOG_INDEX = rf.getLatestLogIndexIncludeSnapshot()
	args.PREV_LOG_TERM = rf.getLatestLogTermIncludeSnapshot()
	rf.mu.Unlock()

	if index == rf.me {
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)
		ans <- reply
		return
	}

	timeout_ms := rf.timeout_const_ms
	max_timeout_times := 10
	done_ch := make(chan struct{}, max_timeout_times)
	send_func := func() {
		for failed_times := 0; failed_times < rf.rpc_retry_times && !rf.killed(); failed_times++ {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(index, args, reply) {
				time.Sleep(time.Duration(rf.rpc_retry_interval_ms) * time.Millisecond)
				rf.mu.Lock()
				if rf.current_term != this_round_term || !rf.statusIs(CANDIDATE) {
					rf.mu.Unlock()
					dPrintf("Server[%d] quit requestOneServerVote for Server[%d]", rf.me, index)
					return
				}
				rf.mu.Unlock()
				continue
			}

			dPrintf("Server[%d] send vote request to server[%d]", rf.me, index)
			ans <- reply
			done_ch <- struct{}{}
			return
		}
	}

	go send_func()

	for timeout_times := 0; timeout_times < max_timeout_times && !rf.killed(); timeout_times++ {
		select {
		case <-done_ch:
			return
		case <-time.After(time.Duration(timeout_ms) * time.Millisecond):
			go send_func()
		}
	}
}

func (rf *Raft) requestOneServerPreVote(index int, ans chan *RequestVoteReply, this_round_term int) {
	args := &RequestVoteArgs{}

	rf.mu.Lock()
	args.TERM = this_round_term + 1
	args.CANDIDATE_ID = rf.me
	args.PREV_LOG_INDEX = rf.getLatestLogIndexIncludeSnapshot()
	args.PREV_LOG_TERM = rf.getLatestLogTermIncludeSnapshot()
	rf.mu.Unlock()

	if index == rf.me {
		reply := &RequestVoteReply{}
		rf.RequestPreVote(args, reply)
		ans <- reply
		return
	}

	timeout_ms := rf.timeout_const_ms
	max_timeout_times := 10
	done_ch := make(chan struct{}, max_timeout_times)
	send_func := func() {
		for failed_times := 0; failed_times < rf.rpc_retry_times && !rf.killed(); failed_times++ {
			reply := &RequestVoteReply{}
			if !rf.sendPreVote(index, args, reply) {
				time.Sleep(time.Duration(rf.rpc_retry_interval_ms) * time.Millisecond)
				rf.mu.Lock()
				if rf.current_term != this_round_term || !rf.statusIs(PRECANDIDATE) {
					rf.mu.Unlock()
					dPrintf("Server[%d] quit requestOneServerPreVote for Server[%d]", rf.me, index)
					return
				}
				rf.mu.Unlock()
				continue
			}

			dPrintf("Server[%d] send pre vote request to server[%d]", rf.me, index)
			ans <- reply
			done_ch <- struct{}{}
			return
		}
	}

	go send_func()
	for timeout_times := 0; timeout_times < max_timeout_times && !rf.killed(); timeout_times++ {
		select {
		case <-done_ch:
			return
		case <-time.After(time.Duration(timeout_ms) * time.Millisecond):
			go send_func()
		}
	}
}

func (rf *Raft) newVote(this_round_term int) {
	rf.mu.Lock()
	if this_round_term != rf.current_term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	dPrintf("Server[%d] new vote", rf.me)

	reply := make(chan *RequestVoteReply, rf.all_server_number)
	for i := 0; i < rf.all_server_number; i++ {
		go rf.requestOneServerVote(i, reply, this_round_term)
	}

	got_tickets := 0
	got_reply := 0
	timeout_ms := 3000 * 10

	for !rf.killed() {
		select {
		case vote_reply := <-reply:
			got_reply++
			dPrintf("Server[%d] got vote reply at term %d, granted is %t", rf.me, this_round_term, vote_reply.VOTE_GRANTED)
			rf.mu.Lock()
			if vote_reply.TERM > rf.current_term {
				rf.becomeCandidate(vote_reply.TERM)
				rf.mu.Unlock()
				return
			}

			// term is new term
			if this_round_term != rf.current_term {
				rf.mu.Unlock()
				dPrintf("Server[%d] quit vote, not term %d", rf.me, this_round_term)
				return
			}

			// current status is not candidate anymore
			if !rf.statusIs(CANDIDATE) {
				rf.mu.Unlock()
				return
			}

			if !vote_reply.VOTE_GRANTED {
				rf.mu.Unlock()
				continue
			}

			got_tickets++
			dPrintf("Server[%d] got vote tickets number is %d", rf.me, got_tickets)

			// tickets not enough
			if got_tickets < rf.quorum_number && got_reply < rf.all_server_number {
				rf.mu.Unlock()
				continue
			}

			if got_tickets < rf.quorum_number && got_reply == rf.all_server_number {
				rf.mu.Unlock()
				dPrintf("Server[%d] lose the vote", rf.me)
				return
			}

			// tickets enough
			rf.becomeLeader()
			rf.mu.Unlock()

			return

		case <-time.After(time.Duration(timeout_ms) * time.Millisecond):
			dPrintf("Server[%d] did not finish vote in time", rf.me)
			return
		}
	}
}

func (rf *Raft) newPreVote(this_round_term int) {
	rf.mu.Lock()
	if this_round_term != rf.current_term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	dPrintf("Server[%d] new pre vote", rf.me)

	reply := make(chan *RequestVoteReply, rf.all_server_number)

	for i := 0; i < rf.all_server_number; i++ {
		go rf.requestOneServerPreVote(i, reply, this_round_term)
	}

	got_tickets := 0
	got_reply := 0
	timeout_ms := 3000 * 10

	for !rf.killed() {
		select {
		case pre_vote_reply := <-reply:
			got_reply++
			rf.mu.Lock()
			if pre_vote_reply.TERM > rf.current_term {
				rf.becomeFollower(pre_vote_reply.TERM, None)
				rf.mu.Unlock()
				return
			}

			// term is new term
			if rf.current_term != this_round_term {
				rf.mu.Unlock()
				dPrintf("Server[%d] quit prevote, not term %d", rf.me, this_round_term)
				return
			}

			// current status is not pre-candidate anymore
			if !rf.statusIs(PRECANDIDATE) {
				rf.mu.Unlock()
				dPrintf("Server[%d] quit prevote, not PRECANDIDATE anymore", rf.me)
				return
			}

			if !pre_vote_reply.VOTE_GRANTED {
				rf.mu.Unlock()
				continue
			}

			got_tickets++
			dPrintf("Server[%d] got prevote tickets number is %d", rf.me, got_tickets)

			// tickets not enough
			if got_tickets < rf.quorum_number && got_reply < rf.all_server_number {
				rf.mu.Unlock()
				continue
			}

			if got_tickets < rf.quorum_number && got_reply == rf.all_server_number {
				rf.mu.Unlock()
				dPrintf("Server[%d] lose the prevote", rf.me)
				return
			}

			// tickets enough
			rf.becomeCandidate(this_round_term + 1)
			rf.mu.Unlock()

			return

		case <-time.After(time.Duration(timeout_ms) * time.Millisecond):
			dPrintf("Server[%d] did not finish prevote in time", rf.me)
			return
		}
	}
}

func (rf *Raft) successAppend(server int, this_round_term int,
	args *RequestAppendEntryArgs) {
	dPrintln("successAppend for", server, "handle args:", args)

	newest_index_in_args := 0
	for _, value := range args.ENTRIES {
		if value.INDEX > newest_index_in_args {
			newest_index_in_args = value.INDEX
		}
	}
	new_next_index := newest_index_in_args + 1
	if new_next_index > rf.next_index[server] {
		dPrintf("leader update next index for Server[%v], new next index is %v", server, new_next_index)
		rf.next_index[server] = new_next_index
	}

	rf.recently_commit <- struct{}{}
}

func (rf *Raft) backwardNewLogPosition(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) int {
	new_prev_log_position, ok := rf.getPositionByIndex(args.PREV_LOG_INDEX - 1)
	if !ok {
		if rf.hasLog() {
			if args.PREV_LOG_INDEX <= rf.log[0].INDEX {
				return -1
			}
		} else {
			dPrintln("backward args find position failed, a error or just cocurrent rpc.")
			return -2
		}
	}

	// peer have at leaest 1 log in args.PREV_LOG_TERM
	if reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM != None {
		// move prev log to previous term's last log
		ok, last_index_of_prev_term := findLastIndexOfTerm(rf.log, args.PREV_LOG_TERM)
		if ok {
			tmp, ok2 := rf.getPositionByIndex(last_index_of_prev_term)
			if ok2 {
				return tmp
			}
		}
	}

	// peer do not have log in args.PREV_LOG_TERM
	if reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM == None {
		ok, last_index_before_term := findLastIndexbeforeTerm(rf.log, args.PREV_LOG_TERM)
		if ok {
			tmp, ok3 := rf.getPositionByIndex(last_index_before_term)
			if ok3 {
				return tmp
			}
		} else {
			if new_prev_log_position >= 0 {
				new_prev_log_position = 0
				return 0
			}
		}
	}

	return -2
}

func (rf *Raft) appendToNewPrevPos(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply, new_prev_log_position int) {
	initil_log_position := rf.logLength() - 1
	new_entries := make([]Log, 0)
	// add logs from latest to prev_log_position to args
	for i := initil_log_position; i > new_prev_log_position; i-- {
		new_entries = append(new_entries, rf.log[i])
	}
	args.ENTRIES = new_entries

	if new_prev_log_position >= 0 {
		new_prev_log_idx := rf.log[new_prev_log_position].INDEX
		args.PREV_LOG_TERM = rf.getLogTermByIndex(new_prev_log_idx)
		args.PREV_LOG_INDEX = new_prev_log_idx
	} else {
		if rf.hasSnapshot() {
			args.PREV_LOG_INDEX = rf.last_log_index_in_snapshot
			args.PREV_LOG_TERM = rf.last_log_term_in_snapshot
		} else {
			args.PREV_LOG_TERM = 0
			args.PREV_LOG_INDEX = 0
		}
	}

}

func (rf *Raft) buildNewestArgs(server int) *RequestAppendEntryArgs {
	args := &RequestAppendEntryArgs{
		TERM:          rf.current_term,
		LEADER_ID:     rf.me,
		LEADER_COMMIT: rf.quorom_commit_index,
	}
	if !rf.hasLog() {
		return args
	}
	latest_log_index := rf.getLatestLogIndex()
	append_logs := make([]Log, 0)
	for idx := latest_log_index; idx >= rf.next_index[server]; idx-- {
		position, ok := rf.getPositionByIndex(idx)
		if !ok {
			break
		}
		append_logs = append(append_logs, rf.log[position])
	}
	args.ENTRIES = append_logs

	if len(args.ENTRIES) == 0 {
		args.PREV_LOG_INDEX = 0
		args.PREV_LOG_TERM = 0

		return args
	}

	prev_log_index := args.ENTRIES[len(args.ENTRIES)-1].INDEX - 1
	prev_log_term := 0
	if prev_log_index > 0 {
		if prev_log_index == rf.last_log_index_in_snapshot {
			prev_log_term = rf.last_log_term_in_snapshot
		} else {
			prev_log_term = rf.getLogTermByIndex(prev_log_index)
		}
	}

	args.PREV_LOG_INDEX = prev_log_index
	args.PREV_LOG_TERM = prev_log_term

	return args
}

func (rf *Raft) sendNewestLog(server int, this_round_term int, ch chan struct{}) {
	defer func() { ch <- struct{}{} }()
	rf.mu.Lock()

	if rf.current_term != this_round_term {
		rf.mu.Unlock()
		return
	}

	if !rf.hasLog() && !rf.hasSnapshot() {
		rf.mu.Unlock()
		return
	}

	if server == rf.me {
		rf.recently_commit <- struct{}{}
		rf.mu.Unlock()
		return
	}

	args := rf.buildNewestArgs(server)

	if len(args.ENTRIES) > 0 && args.ENTRIES[0].INDEX < rf.next_index[server] {
		goto release_lock_and_return
	}

	rf.mu.Unlock()

	// TODO: refactor, extract as a function
	for failed_times := 0; !rf.killed(); failed_times++ {
		reply := &RequestAppendEntryReply{}
		if !rf.sendOneAppendEntry(server, args, reply) {
			return
		}

		rf.mu.Lock()
		// new term case 1, update term by other way
		if this_round_term != rf.current_term {
			goto release_lock_and_return
		}

		// new term case 2, know from peer
		if reply.TERM > rf.current_term {
			rf.becomeFollower(reply.TERM, None)
			goto release_lock_and_return
		}

		if reply.SUCCESS {
			rf.successAppend(server, this_round_term, args)
			goto release_lock_and_return
		}

		if len(args.ENTRIES) > 0 && args.ENTRIES[0].INDEX < rf.next_index[server] {
			goto release_lock_and_return
		}

		dPrintf("Server[%v] send log to %v failed time: %v, args is %v", rf.me, server, failed_times, args)

		need_snapshot := rf.hasSnapshot() &&
			((rf.next_index[server] <= rf.last_log_index_in_snapshot) ||
				(reply.NEWST_LOG_INDEX_OF_PREV_LOG_TERM < rf.last_log_index_in_snapshot)) &&
			(reply.LAST_LOG_INDEX_IN_SNAPSHOT != rf.last_log_index_in_snapshot ||
				reply.LAST_LOG_TERM_IN_SNAPSHOT != rf.last_log_term_in_snapshot)

		if need_snapshot {
			rf.mu.Unlock()
			dPrintf("Server[%v] need snapshot", server)
			rf.sendSnapshot(server, this_round_term)
			return
		}
		// log not match
		is_first_log := args.PREV_LOG_INDEX == 0 && args.PREV_LOG_TERM == 0
		prev_log_is_snapshot_log := args.PREV_LOG_TERM == rf.last_log_term_in_snapshot &&
			args.PREV_LOG_INDEX == rf.last_log_index_in_snapshot
		log_not_exist := args.PREV_LOG_INDEX <= rf.last_log_index_in_snapshot
		if is_first_log || prev_log_is_snapshot_log || log_not_exist {
			goto release_lock_and_return
		}

		new_prev_log_position := rf.backwardNewLogPosition(args, reply)
		rf.appendToNewPrevPos(args, reply, new_prev_log_position)
		dPrintf("Server[%v] log dismatch, new args is %v", server, args)
		rf.mu.Unlock()
	}
	return

release_lock_and_return:
	rf.mu.Unlock()
	return
}

func (rf *Raft) handleAppendEntryForOneServer(server int, this_round_term int) {
	worker_number := 10
	ch := make(chan struct{}, worker_number)
	for i := 0; i < worker_number; i++ {
		ch <- struct{}{}
	}

	for !rf.killed() {
		rf.mu.Lock()
		if rf.current_term != this_round_term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		select {
		case <-time.After(time.Duration(rf.heartbeat_interval_ms) * time.Millisecond):
			<-ch
			go rf.sendNewestLog(server, this_round_term, ch)
		case <-rf.append_entry_chan[server]:
			<-ch
			go rf.sendNewestLog(server, this_round_term, ch)
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// is not leader
	if !rf.statusIs(LEADER) {
		return new_log_index, term, isLeader
	}
	term = rf.current_term
	isLeader = true
	new_log_index = rf.getLatestLogIndexIncludeSnapshot() + 1
	rf.next_index[rf.me] = new_log_index + 1
	new_log := []Log{
		{
			INDEX:   new_log_index,
			TERM:    rf.current_term,
			COMMAND: command,
		},
	}
	rf.appendLogs(new_log)
	go rf.newRoundAppend(command, new_log_index)

	dPrintf("Server[%v] Start accept new log: %v", rf.me, new_log)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dPrintln("Kill Raft server", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) statusIs(status int) bool {
	return rf.status == status
}

func (rf *Raft) becomePreCandidate() {
	rf.leader_id = None
	rf.receive_from_leader = false
	rf.status = PRECANDIDATE
	dPrintf("Server[%d] become pre-candidate", rf.me)
	go rf.newPreVote(rf.current_term)
}

func (rf *Raft) becomeCandidate(new_term int) {
	if new_term <= rf.current_term {
		dPrintf("Server[%d] Error: becomeCandidate in to older term", rf.me)
		return
	}
	// NOTE: always update term first, then update voted for
	rf.setTerm(new_term)
	rf.setVotefor(None)
	rf.status = CANDIDATE
	dPrintf("Server[%d] become candidate at term %d", rf.me, new_term)
	go rf.newVote(rf.current_term)
}

func (rf *Raft) becomeFollower(new_term int, new_leader int) {
	if new_leader == rf.me {
		dPrintf("Server[%d] become follower by itself, impossble", rf.me)
		return
	}
	if rf.status != FOLLOWER {
		dPrintf("Server[%d] become follower, new leader is %d, new term is %d", rf.me, new_leader, new_term)
	}
	rf.status = FOLLOWER

	prev_term := rf.current_term

	rf.setTerm(new_term)

	if prev_term < new_term {
		rf.setVotefor(None)
	}

	if new_leader != None {
		rf.receive_from_leader = true
	} else {
		rf.receive_from_leader = false
	}

	rf.leader_id = new_leader
}

func (rf *Raft) becomeLeader() {
	rf.status = LEADER
	rf.next_index[rf.me] = rf.getLatestLogIndexIncludeSnapshot() + 1

	// NOTE: send heartbeat ASAP
	// TODO: commit a no-op log to promise know commit index
	rf.sendOneRoundHeartBeat()
	go rf.leaderUpdateCommitIndex(rf.current_term)

	for i := 0; i < rf.all_server_number; i++ {
		if i == rf.me {
			continue
		}
		rf.next_index[i] = 1
		go rf.handleOneServerHeartbeat(i, rf.current_term)
		go rf.handleAppendEntryForOneServer(i, rf.current_term)
	}

	dPrintf("Server[%d] become LEADER at term %d", rf.me, rf.current_term)
	return
}

// The Ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		duration := rand.Intn(rf.timeout_rand_ms) + rf.timeout_const_ms
		time.Sleep(time.Duration(duration) * time.Millisecond)

		rf.mu.Lock()

		// if I am leader
		if rf.statusIs(LEADER) {
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
			rf.becomeFollower(rf.current_term, None)

		case CANDIDATE:
			if rf.enable_feature_prevote {
				rf.becomePreCandidate()
			} else {
				rf.becomeCandidate(rf.current_term + 1)
			}
		}

		rf.mu.Unlock()
	}
}

func initLogSetting(me int) {
	file, err := os.OpenFile(LOGPATH, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
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
	// FIXME: this is an arbitray number
	rf.chanel_buffer_size = 100000
	rf.recently_commit = make(chan struct{}, rf.chanel_buffer_size)
	rf.internal_apply_chan = make(chan ApplyMsg, rf.chanel_buffer_size)
	rf.append_entry_chan = make([]chan struct{}, rf.all_server_number)
	rf.apply_ch = applyCh
	rf.rpc_retry_times = 1
	rf.rpc_retry_interval_ms = 10
	rf.heartbeat_interval_ms = 100

	for i := 0; i < rf.all_server_number; i++ {
		rf.next_index[i] = 1
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
