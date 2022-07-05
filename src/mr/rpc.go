package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const MS_IN_NANOSECONDS int = 1000000
const SEC_IN_NANOSECONDS int = 1000000000
const HEART_BEAT_DURATION = SEC_IN_NANOSECONDS

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	JOB_MAP = iota
	JOB_REDUCE
	JOB_NONE
)

type Job struct {
	JOB_ID             int
	FILENAME           string
	JOB_TYPE           int
	FILENAMES          []string
	REDUCE_PART        int
	REDUCE_JOBS_NUMBER int
}

type JobDone struct {
	JOB_ID   int
	FILENAME string
	JOB_TYPE int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
