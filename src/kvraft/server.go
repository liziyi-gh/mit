package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func (kv *KVServer) setNotifier(request_id uint64, notify *applyNotify) {
	kv.notifier[request_id] = notify
	DPrintln("set_notifer_for", request_id)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
		fmt.Print("\n")
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Println(a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       string
	Key        string
	Value      string
	Request_id uint64
	Client_id  uint32
	Trans_id   uint32
}

type applyNotify struct {
	ch      chan struct{}
	op_type string
	key     string
	value   string
	err     Err
}

type raftLog struct {
	op        *Op
	ch        chan string
	notify_ch chan struct{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister

	// Your definitions here.
	chanel_buffer         int
	data                  map[string]string
	notifier              map[uint64](*applyNotify)
	applyed_index         int
	transcation_duplicate map[uint32](uint32)
	client_apply          map[uint32](uint32)
	raft_chan             chan raftLog
}

func (kv *KVServer) calculateRequestId(client_id uint32, trans_id uint32) uint64 {
	return uint64(client_id)<<32 + uint64(trans_id)
}

func (kv *KVServer) checkTransactionDone(client_id uint32, trans_id uint32) bool {
	return kv.client_apply[client_id] >= trans_id
}

func (kv *KVServer) setTransactionDone(client_id uint32, trans_id uint32) {
	if trans_id < kv.client_apply[client_id] {
		DPrintln("setting trans_id < kv.client_apply[client_id]")
	}
	kv.client_apply[client_id] = trans_id
}

func (kv *KVServer) setTransactionDuplicate(client_id uint32, trans_id uint32) {
	kv.transcation_duplicate[client_id] = trans_id
	DPrintln("Server", kv.me, "allocate", "for client", client_id, "trans id", trans_id)
}

func (kv *KVServer) sendRaftLog(raftlog raftLog) {
	op := raftlog.op
	ch := raftlog.ch
	notify_ch := raftlog.notify_ch

	retry_ms := 1000
	retry_times := 10
	for i := 0; i < retry_times; i++ {
		DPrintf("Server[%v] [sendRaftLog] trying_request_id %v", kv.me, op.Request_id)
		kv.mu.Lock()
		done := kv.checkTransactionDone(op.Client_id, op.Trans_id)
		if done {
			kv.mu.Unlock()
			DPrintf("Server[%v] [sendRaftLog] op %v already done", kv.me, op.Request_id)
			// FIXME: here block
			ch <- INTERNAL_ERROR
			return
		}
		kv.mu.Unlock()
		DPrintln("Server", kv.me, "trying sendRaftLog client id", op.Client_id, "trans id", op.Trans_id, "request id", op.Request_id)
		_, _, is_leader := kv.rf.Start(*op)
		if !is_leader {
			ch <- NOTLEADER
			return
		}
		select {
		case <-time.After(time.Duration(retry_ms) * time.Millisecond):
			DPrintln("sendRaftLog timeout, request_id", op.Request_id)
			continue
		case <-notify_ch:
			return
		}
	}
	DPrintln("sendRaftLog internal error, request_id", op.Request_id)
	ch <- INTERNAL_ERROR
}

func (kv *KVServer) sendToRaft() {
	for raftlog := range kv.raft_chan {
		if kv.killed() {
			return
		}
		DPrintf("Server[%v] sendToRaft__start %v", kv.me, raftlog.op.Request_id)
		kv.sendRaftLog(raftlog)
		DPrintf("Server[%v] sendToRaft_finish %v", kv.me, raftlog.op.Request_id)
	}
}

func (kv *KVServer) sendOneOp(request_id uint64, op *Op) (chan string, *applyNotify) {
	// FIXME: magic number here
	ch := make(chan string, 10)
	notify_ch := make(chan struct{})
	notify := &applyNotify{
		ch: notify_ch,
	}

	kv.setNotifier(request_id, notify)

	raftlog := raftLog{
		op:        op,
		ch:        ch,
		notify_ch: notify_ch,
	}
	DPrintln("[Server] [sendOneOp] trying ", request_id, "in server", kv.me)
	kv.raft_chan <- raftlog
	DPrintln("[Server] [sendOneOp] sent ", request_id, "in server", kv.me)

	return ch, notify
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	request_id := kv.calculateRequestId(args.Client_id, args.Trans_id)
	reply.RequestId = request_id
	done := kv.checkTransactionDone(args.Client_id, args.Trans_id)
	if done {
		cache, ok := kv.notifier[request_id]
		if ok {
			DPrintln("returning cache for request_id", request_id)
			reply.Err = DUPLICATE_GET
			reply.Value = cache.value
			kv.mu.Unlock()
			DPrintln("duplicate Get RPC from client", args.Client_id, "trans_id", args.Trans_id)
			return
		} else {
			// Snapshot 导致有的服务器没有跟上 之前处理这个请求的服务器已经不是 leader 了
			// Server 处理完了 RPC 时刚好挂了 Client 没有收到回复 但是 Raft 集群已经处理完了
			// 这样直接再做一次只能保证同一个 Clerk 看到的结果的一致性 无法保证线性一致性?
			// probably not
			kv.setTransactionDone(args.Client_id, args.Trans_id-1)
			DPrintf("Server[%v] no_cache for request_id %v", kv.me, request_id)
		}
	}

	op := Op{
		Type:       "Get",
		Key:        args.Key,
		Client_id:  args.Client_id,
		Trans_id:   args.Trans_id,
		Request_id: request_id,
	}
	ch, notify := kv.sendOneOp(request_id, &op)
	kv.mu.Unlock()

	DPrintln("Server", kv.me, "get_start_select_for", request_id)

	defer func() {
		kv.mu.Lock()
		delete(kv.notifier, request_id)
		kv.mu.Unlock()
		DPrintln("Server", kv.me, "get_finish_select_for", request_id)
	}()

	select {

	case err := <-ch:
		DPrintln("[Server] [Get] error", err)
		reply.Err = Err(err)
		return

	case <-notify.ch:
		reply.Value = notify.value
		reply.Err = notify.err
		DPrintln("Get receive notify for requestID", op.Request_id)
		DPrintln("[Server] [Get] lzy: return value is", reply.Value)
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	done := kv.checkTransactionDone(args.Client_id, args.Trans_id)
	if done {
		kv.mu.Unlock()
		reply.Err = Err(DUPLICATE_PUTAPPEND)
		DPrintln("duplicate PutAppend RPC client id is", args.Client_id, "trans id is", args.Trans_id)
		return
	}

	request_id := kv.calculateRequestId(args.Client_id, args.Trans_id)

	op := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Client_id:  args.Client_id,
		Trans_id:   args.Trans_id,
		Request_id: request_id,
	}

	ch, notify := kv.sendOneOp(request_id, &op)

	kv.mu.Unlock()
	DPrintln("Server", kv.me, "putappend_start_select_for", request_id)

	defer func() {
		kv.mu.Lock()
		delete(kv.notifier, request_id)
		kv.mu.Unlock()
		DPrintln("Server", kv.me, "putappend_finish_select_for", request_id)
	}()

	select {

	case err := <-ch:
		DPrintln("[Server] [PutAppend] error", err)
		reply.Err = Err(err)
		return

	case <-notify.ch:
		reply.Err = notify.err
		DPrintln("PutAppend receive notify for requestID", op.Request_id)
		DPrintln("[Server] [PutAppend] return with err", reply.Err)
		return
	}
}

func (kv *KVServer) applyCommand(command raft.ApplyMsg) {
	kv.mu.Lock()

	op := command.Command.(Op)
	DPrintln("[applier] op is", op)

	if kv.checkTransactionDone(op.Client_id, op.Trans_id) {
		DPrintln("Server ", kv.me, "alreay done", op.Request_id)
		kv.mu.Unlock()
		return
	}

	notify, need_notify := kv.notifier[op.Request_id]

	switch op.Type {
	case "Get":
		if need_notify {
			DPrintln("[Server]", kv.me, "[applier] get", op.Key, "as", kv.data[op.Key])
			notify.value = kv.data[op.Key]
		}
	case "Put":
		DPrintln("[Server]", kv.me, "[applier] set", op.Key, "to", op.Value)
		kv.data[op.Key] = op.Value
	case "Append":
		origin_value, ok := kv.data[op.Key]
		if !ok {
			DPrintln("[Server]", kv.me,
				"[applier] update", op.Key, "to", op.Value)
			kv.data[op.Key] = op.Value
		} else {
			DPrintln("[Server]", kv.me,
				"[applier] update", op.Key, "to", origin_value+op.Value)
			kv.data[op.Key] = origin_value + op.Value
		}
	}

	kv.applyed_index = command.CommandIndex
	kv.setTransactionDone(op.Client_id, op.Trans_id)
	kv.setTransactionDuplicate(op.Client_id, op.Trans_id)
	if need_notify {
		close(notify.ch)
	}

	DPrintln("[Server]", kv.me,
		"[applier] success apply",
		op.Request_id, "raft index", command.CommandIndex)

	kv.mu.Unlock()
}

func (kv *KVServer) readSnapshot(snapshot_data []byte) {
	r := bytes.NewBuffer(snapshot_data)
	d := labgob.NewDecoder(r)
	m := make(map[string]string)
	apply_index := 0
	transcation_duplicate := make(map[uint32](uint32))
	client_apply := make(map[uint32](uint32))

	ok := d.Decode(&m) == nil &&
		d.Decode(&apply_index) == nil &&
		d.Decode(&transcation_duplicate) == nil &&
		d.Decode(&client_apply) == nil
	if ok {
		kv.data = m
		kv.applyed_index = apply_index
		kv.transcation_duplicate = transcation_duplicate
		kv.client_apply = client_apply
		DPrintf("Server[%v] read snapshot success", kv.me)
	}
}

func (kv *KVServer) applySnapshot(command raft.ApplyMsg) {
	kv.mu.Lock()
	DPrintf("Server[%v] applySnapshot", kv.me)
	kv.readSnapshot(command.Snapshot)
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for command := range kv.applyCh {
		if kv.killed() {
			return
		}
		DPrintln("Server", kv.me, "[applier] get command", command)
		if command.CommandValid {
			kv.applyCommand(command)
		}

		if command.SnapshotValid {
			kv.applySnapshot(command)
		}

		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.data)
			e.Encode(kv.applyed_index)
			e.Encode(kv.transcation_duplicate)
			e.Encode(kv.client_apply)
			data := w.Bytes()
			kv.rf.Snapshot(kv.applyed_index, data)
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.chanel_buffer = 10000
	kv.data = make(map[string]string)
	kv.notifier = make(map[uint64](*applyNotify))
	kv.persister = persister
	kv.transcation_duplicate = make(map[uint32](uint32))
	kv.client_apply = make(map[uint32](uint32))
	kv.raft_chan = make(chan raftLog, kv.chanel_buffer)
	snapshot_data := persister.ReadSnapshot()
	kv.readSnapshot(snapshot_data)
	go kv.applier()
	go kv.sendToRaft()

	// You may need initialization code here.

	return kv
}
