package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	Sleep TaskType = iota
	Map
	Reduce
)

type WorkStatus int

const (
	Completed WorkStatus = iota
	Failed
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type CoordinatorCallArgs struct {
	TaskType   TaskType
	WorkStatus WorkStatus
	TaskId     int
	Filename   string
}

type CoordinatorCallReply struct {
	TaskType TaskType
	TaskId   int
	Filename string
	NReduce  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
