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

const (
	Die Operation = iota
	RunMap
	RunReduce
)

type Operation uint

type (
	// Tell coordinator we are ready for work
	IdleArgs  struct{}
	IdleReply struct {
		Op Operation
	}

	// Tell coordinator we are ready to run map
	MapArgs  struct{}
	MapReply struct {
		InFile        string
		MapTaskID     int
		NumReduceTask int
	}

	// Tell coordinator we finished the map job
	DoneMapArgs struct {
		MapTaskID int
	}
	DoneMapReply struct{}

	// Tell coordinator we are ready to run reduce
	ReduceArgs  struct{}
	ReduceReply struct {
		ReduceTaskID int
		NumMapTask   int
	}

	// Tell coordinator we finished the reduce job
	DoneReduceArgs struct {
		ReduceTaskID int
	}
	DoneReduceReply struct{}
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	return "/var/tmp/824-mr-" + strconv.Itoa(os.Getuid())
}
