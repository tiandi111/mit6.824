package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	None TaskType = iota
	Map
	Reduce
	Done
)

type TaskState int

const (
	New TaskState = iota
	Running
	Succeeded
	Failed
)

type MapReduceArgs struct {
	TaskID int
	TaskType
	TaskState
}

type MapReduceReply struct {
	TaskType
	TaskID  int
	NReduce int
	MapFile string
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
