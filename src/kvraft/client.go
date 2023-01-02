package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

var used_me_number_lock sync.Mutex
var used_me_number map[uint32](bool) = make(map[uint32](bool))

const RPC_RETRY_TIMES = 50
const RPC_WAIT_TIME_MS = 500

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	leader   int
	trans_id uint32
	me       uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// FIXME: what if client id run out? add close
	used_me_number_lock.Lock()
	for {
		i := uint32(nrand())
		_, ok := used_me_number[i]
		if !ok {
			used_me_number[i] = true
			ck.me = i
			break
		}
	}
	used_me_number_lock.Unlock()
	DPrintln("Make clerk")
	return ck
}

// TODO: reply should contain leader id
func (ck *Clerk) changeLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) getTransId() uint32 {
	ck.trans_id += 1
	return ck.trans_id
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &GetArgs{
		Key:       key,
		Client_id: ck.me,
		Trans_id:  ck.getTransId(),
	}
	not_leader_time := 0
	rpc_failed_times := 0
	for i := 0; i < RPC_RETRY_TIMES; i++ {
		DPrintln("[Client] trying Get", "client", ck.me, "trans id", args.Trans_id)
		reply := &GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		if !ok {
			ck.changeLeader()
			rpc_failed_times += 1
			time.Sleep(RPC_WAIT_TIME_MS * time.Millisecond)
			continue
		}

		if reply.Err == NOTLEADER {
			not_leader_time += 1
			time.Sleep(RPC_WAIT_TIME_MS * time.Millisecond)
			ck.changeLeader()
			continue
		}

		if reply.Err == INTERNAL_ERROR {
			not_leader_time += 1
			time.Sleep(RPC_WAIT_TIME_MS * time.Millisecond)
			ck.changeLeader()
			continue
		}

		DPrintln("[Client] Receive reply for Get_request", reply.RequestId, "err is", reply.Err)

		if reply.Err == "" {
			return reply.Value
		}

		if reply.Err == DUPLICATE_GET {
			DPrintln("[Client] returning cache")
			return reply.Value
		}

		DPrintln("[Client] Get error: ", reply.Err)
		return GET_ERROR_REPLY
	}

	// You will have to modify this function.
	return GET_ERROR_REPLY
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &PutAppendArgs{
		Op:        op,
		Key:       key,
		Value:     value,
		Client_id: ck.me,
		Trans_id:  ck.getTransId(),
	}
	not_leader_times := 0
	rpc_failed_times := 0
	for i := 0; i < RPC_RETRY_TIMES; i++ {
		DPrintln("[Client] trying PutAppend", "client", ck.me, "trans id", args.Trans_id)
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
		if !ok {
			ck.changeLeader()
			rpc_failed_times += 1
			time.Sleep(RPC_WAIT_TIME_MS * time.Millisecond)
			continue
		}
		DPrintln("[Client] PutAppend err is", reply.Err)
		if reply.Err == "" {
			DPrintln("[Client] PutAppend success")
			return
		}

		if reply.Err == DUPLICATE_PUTAPPEND {
			DPrintln("[Client]", DUPLICATE_PUTAPPEND)
			return
		}

		if reply.Err == NOTLEADER {
			not_leader_times += 1
			DPrintln("[Client] PutAppend not leader")
			time.Sleep(RPC_WAIT_TIME_MS * time.Millisecond)
			ck.changeLeader()
			continue
		}

		if reply.Err == INTERNAL_ERROR {
			time.Sleep(RPC_WAIT_TIME_MS * time.Millisecond)
			continue
		}

		DPrintln("[Client] PutAppend error:", reply.Err)
		return
	}
	DPrintln("[Client] PutAppend final error, not_leader_times is, rpc_faild_times is", not_leader_times, rpc_failed_times)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
