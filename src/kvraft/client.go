package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	leader     int
	request_id uint64
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
	return ck
}

func (ck *Clerk) getRequestID() uint64 {
	ck.request_id += 1
	return ck.request_id
}

func (ck *Clerk) changeLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) getLeader() int {
	return ck.leader
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
		RequestID: ck.getRequestID(),
	}
	for i := 0; i < 20; i++ {
		log.Println("trying Get")
		reply := &GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		if !ok {
			ck.changeLeader()
			continue
		}

		if reply.Err == "" {
			return reply.Value
		}

		if reply.Err == "Not leader" {
			time.Sleep(200 * time.Millisecond)
			ck.changeLeader()
			continue
		}

		log.Println("return Get")
		return ""

	}
	log.Println("Get failed: can not find leader")

	// You will have to modify this function.
	return ""
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
	log.Println("PutAppend value is", value)
	args := &PutAppendArgs{
		Op:        op,
		Key:       key,
		Value:     value,
		RequestID: ck.getRequestID(),
	}
	for i := 0; i < 20; i++ {
		log.Println("trying PutAppend", args.RequestID, "to server", ck.getLeader())
		reply := &PutAppendReply{}
		ok := ck.servers[ck.getLeader()].Call("KVServer.PutAppend", args, reply)
		if !ok {
			ck.changeLeader()
			continue
		}
		log.Println("[Client] PutAppend err is", reply.Err)
		if reply.Err == "" {
			log.Println("PutAppend success")
			return
		}
		if reply.Err == "Not leader" {
			log.Println("PutAppend not leader")
			time.Sleep(200 * time.Millisecond)
			ck.changeLeader()
			continue
		}
		log.Println("PutAppend failed:", reply.Err)
		return
	}
	log.Println("PutAppend failed: can not find leader")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
