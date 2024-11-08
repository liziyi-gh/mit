package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id uint32
	Trans_id  uint32
}

type PutAppendReply struct {
	Err     Err
	Leader  int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_id uint32
	Trans_id  uint32
}

type GetReply struct {
	Err     Err
	Value   string
	Leader  int
	RequestId uint64
}

const NOTLEADER = "Not leader"
const INTERNAL_ERROR = "Internal error"
const GET_ERROR_REPLY = ""
const DUPLICATE_GET = "Duplicate get"
const DUPLICATE_GET_NO_CACHE = "Duplicate get, no_cache"
const DUPLICATE_GET_NOT_DONE = "Duplicate get, not_done"
const DUPLICATE_PUTAPPEND = "Duplicate putappend"
