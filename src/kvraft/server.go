package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
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
		panic("trans_id < kv.client_apply[client_id]")
	}
	kv.client_apply[client_id] = trans_id
}

func (kv *KVServer) checkTransactionDuplicate(client_id uint32, trans_id uint32) bool {
	value := kv.transcation_duplicate[client_id]
	return value >= trans_id
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
		kv.mu.Lock()
		duplicate := kv.checkTransactionDuplicate(op.Client_id, op.Trans_id)
		if duplicate {
			kv.mu.Unlock()
			DPrintln("[sendRaftLog] duplicate op", op)
			return
		}
		kv.mu.Unlock()
		DPrintln("Server", kv.me, "trying Start client id", op.Client_id, "trans id", op.Trans_id, "request id", op.Request_id)
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
			kv.mu.Lock()
			kv.setTransactionDuplicate(op.Client_id, op.Trans_id)
			kv.mu.Unlock()
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
		DPrintln("sendToRaft new raftlog")
		kv.sendRaftLog(raftlog)
	}
}

func (kv *KVServer) sendOneOp(request_id uint64, op *Op) (chan string, *applyNotify) {
	ch := make(chan string)
	notify_ch := make(chan struct{})
	notify := &applyNotify{
		ch: notify_ch,
	}

	kv.notifier[request_id] = notify

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
	reply.Receive = true

	kv.mu.Lock()
	duplicate := kv.checkTransactionDuplicate(args.Client_id, args.Trans_id)
	if duplicate {
		request_id := kv.calculateRequestId(args.Client_id, args.Trans_id)
		done := kv.checkTransactionDone(args.Client_id, args.Trans_id)
		if done {
			DPrintln("returning cache")
			reply.Err = DUPLICATE_GET
			reply.Value = kv.notifier[request_id].value
		} else {
			reply.Err = Err("cache can not find")
		}
		kv.mu.Unlock()
		DPrintln("duplicate Get RPC")
		return
	}
	request_id := kv.calculateRequestId(args.Client_id, args.Trans_id)

	op := Op{
		Type:       "Get",
		Key:        args.Key,
		Client_id:  args.Client_id,
		Trans_id:   args.Trans_id,
		Request_id: request_id,
	}
	ch, notify := kv.sendOneOp(request_id, &op)
	kv.mu.Unlock()

	select {

	case err := <-ch:
		DPrintln("[Server] [Get] error", err)
		reply.Err = Err(err)
		return

	case <-notify.ch:
		reply.Value = notify.value
		reply.Err = notify.err
		DPrintln("receive notify for requestID", op.Request_id)
		DPrintln("[Server] [Get] lzy: return value is", reply.Value)
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Receive = true

	kv.mu.Lock()
	duplicate := kv.checkTransactionDuplicate(args.Client_id, args.Trans_id)
	if duplicate {
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

	select {

	case err := <-ch:
		DPrintln("[Server] [PutAppend] error", err)
		reply.Err = Err(err)
		return

	case <-notify.ch:
		reply.Err = notify.err
		DPrintln("receive notify for requestID", op.Request_id)
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

	notify, ok := kv.notifier[op.Request_id]

	if !ok {
		tmp_notify_ch := make(chan struct{})
		tmp_notify := &applyNotify{
			ch: tmp_notify_ch,
		}
		kv.notifier[op.Request_id] = tmp_notify
		notify = tmp_notify
	}

	switch op.Type {
	case "Get":
		DPrintln("[Server]", kv.me, " [applier] get", op.Key, "as", kv.data[op.Key])
		notify.value = kv.data[op.Key]
	case "Put":
		DPrintln("[Server]", kv.me, " [applier] set", op.Key, "to", op.Value)
		kv.data[op.Key] = op.Value
	case "Append":
		origin_value, ok := kv.data[op.Key]
		if !ok {
			DPrintln("[Server]", kv.me, " [applier] update", op.Key, "to", op.Value)
			kv.data[op.Key] = op.Value
		} else {
			DPrintln("[Server]", kv.me, " [applier] update", op.Key, "to", origin_value+op.Value)
			kv.data[op.Key] = origin_value + op.Value
		}
	}

	kv.applyed_index = command.CommandIndex
	kv.setTransactionDone(op.Client_id, op.Trans_id)
	close(notify.ch)

	DPrintln("[Server]", kv.me, " [applier] success apply", op.Request_id, "raft index", command.CommandIndex)

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
	}
}

func (kv *KVServer) applySnapshot(command raft.ApplyMsg) {
	kv.mu.Lock()
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
