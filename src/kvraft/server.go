package kvraft

import (
	"bytes"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	RequestID uint64
}

type applyNotify struct {
	ch      chan struct{}
	op_type string
	key     string
	value   string
	err     Err
	done    bool
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
	chanel_buffer int
	data          map[string]string
	notifier      map[uint64](*applyNotify)
	applyed_index int
}

func (kv *KVServer) sendOp(op Op, ch chan string, notify_ch chan struct{}) {
	retry_ms := 1000
	retry_times := 10
	for i := 0; i < retry_times; i++ {
		_, _, is_leader := kv.rf.Start(op)
		if !is_leader {
			ch <- "Not leader"
		}
		select {
		case <-time.After(time.Duration(retry_ms) * time.Millisecond):
			continue
		case <-notify_ch:
			return
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		RequestID: args.RequestID,
	}
	ch := make(chan string)
	notify_ch := make(chan struct{})
	notify := &applyNotify{
		ch: notify_ch,
	}

	kv.mu.Lock()
	kv.notifier[args.RequestID] = notify
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifier, args.RequestID)
		kv.mu.Unlock()
	}()

	log.Println("[Server] [Get] send Get")
	go kv.sendOp(op, ch, notify_ch)

	select {

	case err := <-ch:
		log.Println("Not leader")
		reply.Err = Err(err)
		return

	case <-kv.notifier[args.RequestID].ch:
		reply.Value = kv.notifier[args.RequestID].value
		reply.Err = kv.notifier[args.RequestID].err
		log.Println("receive notify for requestID", op.RequestID)
		log.Println("[Server] [Get] return with err", reply.Err)
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		RequestID: args.RequestID,
	}
	ch := make(chan string)
	notify_ch := make(chan struct{})
	notify := &applyNotify{
		ch: notify_ch,
	}

	kv.mu.Lock()
	kv.notifier[args.RequestID] = notify
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifier, args.RequestID)
		kv.mu.Unlock()
	}()

	log.Println("[Server] [PutAppend] send Get")
	go kv.sendOp(op, ch, notify_ch)

	select {

	case err := <-ch:
		log.Println("Not leader")
		reply.Err = Err(err)
		return

	case <-notify.ch:
		reply.Err = kv.notifier[args.RequestID].err
		log.Println("receive notify for requestID", op.RequestID)
		log.Println("[Server] [PutAppend] return with err", reply.Err)
		return
	}
}

func (kv *KVServer) applier() {
	for command := range kv.applyCh {
		log.Println("[applier] get command", command)
		if command.CommandValid {
			kv.mu.Lock()

			op := command.Command.(Op)
			log.Println("[applier] op is", op)

			notify, ok := kv.notifier[op.RequestID]
			if !ok || notify == nil || notify.done {
				log.Println("alreay done")
				kv.mu.Unlock()
				continue
			}

			switch op.Type {
			case "Get":
				log.Println("[Server] [applier] get", op.Key, "as", kv.data[op.Key]+op.Value)
				notify.value = kv.data[op.Key]
			case "Put":
				log.Println("[Server] [applier] put", op.Key, "to", kv.data[op.Key]+op.Value)
				kv.data[op.Key] = op.Value
			case "Append":
				log.Println("[Server] [applier] update", op.Key, "to", kv.data[op.Key]+op.Value)
				kv.data[op.Key] = kv.data[op.Key] + op.Value
			}

			log.Println("send notify for requestID", op.RequestID)
			notify.done = true
			kv.applyed_index = command.CommandIndex
			close(notify.ch)

			log.Println("[Server]", kv.me, " [applier] success apply", op.RequestID)

			kv.mu.Unlock()
		}

		if command.SnapshotValid {
			kv.mu.Lock()
			r := bytes.NewBuffer(command.Snapshot)
			d := labgob.NewDecoder(r)
			m := make(map[string]string)
			apply_index := 0
			ok := d.Decode(&m) == nil && d.Decode(apply_index) == nil
			if ok {
				kv.data = m
				kv.applyed_index = apply_index
			}
			kv.mu.Unlock()
		}

		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.data)
			e.Encode(kv.applyed_index)
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
	go kv.applier()

	// You may need initialization code here.

	return kv
}
